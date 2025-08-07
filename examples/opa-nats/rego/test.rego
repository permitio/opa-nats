package test

import rego.v1

default bucket_watched := false

bucket_watched := nats.kv.watch_bucket(input.bucket_id)

# Simple test policy to verify OPA is working
x := result if {
    bucket_watched
    result := data.nats.kv[input.bucket_id]
} else := result if {
    not bucket_watched
    result := nats.kv.get_data(input.bucket_id, "members")
}
