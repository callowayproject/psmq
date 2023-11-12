#!lua name=psmq
-- This is the library of functions that are used by the psmq scripts.

-- "QUEUES" (queue_set_key) is a set of all the queues.
--   The global SET, which stores all used queue names. When a queue is created the name is added to this set as a
--   member. When a queue is deleted the member will be removed from this set.

-- <queue_name>:Q (queue_info_key) is a hash of queue info.
--   This hash keeps all data for a single queue.
--
--   FIELDS
--
--   {msgid}: The message
--   {msgid}:rc: The receive counter for a single message. Will be incremented on each receive.
--   {msgid}:fr: The timestamp when this message was received for the first time. Will be created on the first receive.
--   totalsent: The total number of messages sent to this queue.
--   totalrecv: The total number of messages received from this queue.
--   vt: The visibility timeout for this queue.
--   initial_delay: The initial delay for messages in this queue.
--   maxsize: The maximum size of this queue.
--   created: The timestamp when this queue was created.
--   modified: The timestamp when this queue was last modified.

--  <qname> (queue_key) A sorted set (ZSET) of all messages of a single queue
--
--    SCORE Next possible timestamp (epoch time in ms) this message can be received.
--
--    MEMBER The `{msgid}'



--
-- Utility functions
--

local alphabet = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
}

local queue_defaults = { viz_timeout = 60, initial_delay = 0, max_size = 65565, retries = 5 }


-- convert the number to base36
local function b36encode(keys)
    local num = tonumber(unpack(keys))

    -- Check for number
    if type(num) == "nil" then
        return redis.error_reply("ERR Number must be a number, not a string. Silly user.")
    end

    -- We can only accept positive numbers
    if num < 0 then
        return redis.error_reply("ERR Number must be a positive value.")
    end

    -- Special case for numbers less than 36
    if num < 36 then
        return alphabet[num + 1]
    end

    -- Process large numbers now
    local result = ""
    while num ~= 0 do
        local i = num % 36
        result = alphabet[i + 1] .. result
        num = math.floor(num / 36)
    end
    return result
end

redis.register_function("b36encode", b36encode)


-- convert the base36 string to a number
local function b36decode(keys)
    local b36 = keys[1]
    return tonumber(b36, 36)
end

redis.register_function("b36decode", b36decode)


-- get time in microseconds and milliseconds
local function get_time()
    local time = redis.call("TIME")
    local timestamp_microsec = time[1] * 1000000 + time[2]
    local timestamp_millisec = math.floor(timestamp_microsec / 1000)
    return { microsec = timestamp_microsec, millisec = timestamp_millisec }
end

redis.register_function("get_time", get_time)


-- make a message id
local function make_message_id(keys)
    local timestamp_microsec = keys[1]
    local message_id = { b36encode({ timestamp_microsec }) }
    for i = 2, 23 do
        message_id[i] = alphabet[math.random(1, 36)]
    end
    return table.concat(message_id)
end

redis.register_function("make_message_id", make_message_id)

--
-- Queue functions
--

-- Create a queue.
-- Returns 1 if the queue was created, 0 if it already existed.
local function create_queue(keys)
    local queues_set_key = "QUEUES"
    local queue_name = keys[1]
    local queue_info_key = queue_name .. ":Q"
    local viz_timeout = keys[2] or queue_defaults.viz_timeout
    local initial_delay = keys[3] or queue_defaults.initial_delay
    local max_size = keys[4] or queue_defaults.max_size
    assert(type(viz_timeout) ~= "nil", "Visibility timeout is nil")
    assert(type(initial_delay) ~= "nil", "initial delay is nil")
    assert(type(max_size) ~= "nil", "max size is nil")

    -- Create the queue.
    local already_existed = redis.call("SADD", queues_set_key, queue_name)
    if already_existed == 0 then
        return 0
    end

    -- Create the queue info hash since it doesn't exist.
    local time = get_time()
    local queue_info = {
        "vt", viz_timeout,
        "delay", initial_delay,
        "maxsize", max_size,
        "created", time.millisec,
        "modified", time.millisec,
        "totalrecv", 0,
        "totalsent", 0,
    }
    redis.call("HMSET", queue_info_key, unpack(queue_info))
    return 1
end

redis.register_function("create_queue", create_queue)

-- Get queue info.
local function get_queue_info(keys)
    local queue_name = keys[1]

    create_queue({ queue_name })

    local queue_info_key = queue_name .. ":Q"
    local queue_info_keys = {
        "vt",
        "delay",
        "maxsize",
        "created",
        "modified",
        "totalrecv",
        "totalsent"
    }

    local raw_info = redis.call("HMGET", queue_info_key, unpack(queue_info_keys))
    local queue_info = {}
    for i, v in ipairs(raw_info) do
        queue_info[queue_info_keys[i]] = v
    end

    local num_msgs = redis.call("ZCARD", queue_name)
    local time_info = get_time()
    local num_hidden = redis.call("ZCOUNT", queue_name, time_info.millisec, "+inf")

    queue_info.num_msgs = num_msgs
    queue_info.num_hidden = num_hidden

    return queue_info
end

local function get_queue_info_for_redis(keys)
    local queue_info = get_queue_info(keys)
    local unp = {}
    for k, v in pairs(queue_info) do
        unp[#unp + 1] = k
        unp[#unp + 1] = v
    end
    return unp
end

redis.register_function("get_queue_info", get_queue_info_for_redis)

-- Set queue visibility timeout
local function set_queue_viz_timeout(keys)
    local queue_name = keys[1]
    local viz_timeout = keys[2]

    create_queue({ queue_name })

    local queue_info_key = queue_name .. ":Q"
    redis.call("HSET", queue_info_key, "vt", viz_timeout)
end

redis.register_function("set_queue_viz_timeout", set_queue_viz_timeout)


-- Set queue initial delay
local function set_queue_initial_delay(keys)
    local queue_name = keys[1]
    local initial_delay = keys[2]

    create_queue({ queue_name })

    local queue_info_key = queue_name .. ":Q"
    redis.call("HSET", queue_info_key, "delay", initial_delay)
end

redis.register_function("set_queue_initial_delay", set_queue_initial_delay)

-- Set queue max size
local function set_queue_max_size(keys)
    local queue_name = keys[1]
    local max_size = keys[2]

    create_queue({ queue_name })

    local queue_info_key = queue_name .. ":Q"
    redis.call("HSET", queue_info_key, "maxsize", max_size)
end

redis.register_function("set_queue_max_size", set_queue_max_size)

--
-- Message functions
--

-- Send a message to a queue.
local function push_message(keys)
    local queue_key = keys[1]
    local message = keys[2]
    local delay = keys[3]

    -- Create the queue in case it doesn't exist.
    create_queue({ queue_key })
    local queue_info = get_queue_info({ queue_key })
    assert(type(queue_info.delay) ~= "nil", "Queue delay is nil")
    local queue_info_key = queue_key .. ":Q"
    local time = get_time()
    local message_id = make_message_id({ time.microsec })
    local message_score = time.millisec + ((delay or queue_info.delay) * 1000)

    -- Add the message to the queue.
    redis.call("ZADD", queue_key, message_score, message_id)

    -- Add the message to the queue info hash.
    redis.call("HSET", queue_info_key, message_id, message)

    -- Increase the total message count for the queue.
    redis.call("HINCRBY", queue_info_key, "totalsent", 1)

    return message_id
end

redis.register_function("push_message", push_message)


-- Get the next message from the queue.
local function get_message(keys)
    local queue_key = keys[1]
    local time = get_time()
    local queue_info = get_queue_info({ queue_key })
    local viz_timeout = (keys[2] or queue_info.vt) * 1000
    local queue_info_key = queue_key .. ":Q"
    local msg = redis.call("ZRANGE", queue_key, "-inf", time.millisec, "BYSCORE", "LIMIT", "0", "1")

    if #msg == 0 then
        return {}
    end

    local message_id = msg[1]
    local message_rc_key = message_id .. ":rc"  -- rc = receive count
    local message_fr_key = message_id .. ":fr"  -- fr = first received

    -- Increase the score of the message by viz_timeout.

    redis.call("ZADD", queue_key, "INCR", viz_timeout, msg[1])

    -- increment the total received count for the queue
    redis.call("HINCRBY", queue_info_key, "totalrecv", 1)

    -- get the message and increment the receive count
    local msg_body = redis.call("HGET", queue_info_key, message_id)
    local rc = redis.call("HINCRBY", queue_info_key, message_rc_key, 1)

    local output = { msg_id = message_id, msg_body = msg_body, rc = rc }

    -- if this is the first time receiving the message, record the timestamp as the first received
    if rc == 1 then
        redis.call("HSET", queue_info_key, message_fr_key, time.millisec)
        output["fr"] = time.millisec
    else
        local fr = redis.call("HGET", queue_info_key, message_fr_key)
        output["fr"] = fr
    end

    return output
end

local function get_message_for_redis(keys)
    local queue_info = get_message(keys)
    local unp = {}
    for k, v in pairs(queue_info) do
        unp[#unp + 1] = k
        unp[#unp + 1] = v
    end
    return unp
end

redis.register_function("get_message", get_message_for_redis)


-- Delete a message from the queue.
local function delete_message(keys)
    local queue_key = keys[1]
    local message_id = keys[2]
    local queue_info_key = queue_key .. ":Q"
    local message_rc_key = message_id .. ":rc"  -- rc = receive count
    local message_fr_key = message_id .. ":fr"  -- fr = first received

    -- remove the message from the queue
    redis.call("ZREM", queue_key, message_id)
    redis.call("HDEL", queue_info_key, message_id, message_rc_key, message_fr_key)
end

redis.register_function("delete_message", delete_message)


-- Pop a message from the queue.
local function pop_message(keys)
    local queue_key = keys[1]
    local time = get_time()

    local message = get_message({ queue_key, time.millisec, 0 })

    local message_id = message.msg_id
    delete_message({ queue_key, message_id })

    return message
end

local function pop_message_for_redis(keys)
    local queue_info = pop_message(keys)
    local unp = {}
    for k, v in pairs(queue_info) do
        unp[#unp + 1] = k
        unp[#unp + 1] = v
    end
    return unp
end

redis.register_function("pop_message", pop_message_for_redis)
