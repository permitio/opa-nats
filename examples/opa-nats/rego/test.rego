package test

import rego.v1

# input.bucket_id identifies the tenant whose slice of the single muxed bucket
# we want. The Rego interface is unchanged from the bucket-per-tenant model: the
# value is now the tenant token.
#
# Placement: a normal (watched) tenant's data is injected at
# data.nats.kv.<bucket_id>.<...>. The one exception is the configured
# `root_tenant` (e.g. permit-groups-default), which is mounted at the OPA data
# ROOT with its tenant token stripped — so its keys land at data.<...>, not
# under data.nats.kv. This example addresses non-root tenants below.
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
