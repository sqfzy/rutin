local function get_value(key)
	-- This will throw an error if the key does not exist or if there's any other error
	return redis.call("GET", key)
end

local result = get_value("non_existing_key")
return result
