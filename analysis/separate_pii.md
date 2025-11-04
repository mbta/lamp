# Protecting PII in LAMP Parquet files
crunkel@mbta.com
2025-11-03

Properly handling personal identifiable information (PII) means
introducing role-based access control (RBAC). We can manage that
straightforwardly using Terraform and AWS by defining policies. Here’s
[the policy to list objects in LAMP buckets with names that start with
`lamp/`](https://github.com/mbta/devops/blob/d31fb0a786efb265256ffc30104bdd063be36081/terraform/aws-tid-main-restricted/iam-policies-groups-lamp_s3_readonly.tf#L66-L76):

``` terraform
  statement {
    actions   = ["s3:ListBucket"]
    resources = local.lamp_buckets

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["lamp/*", "lamp"]
    }
    sid = "AllowListingOfLampBuckets"
  }
```

Terraform can interpret more complex conditions, such as string matching
against s3 prefixes or delimiters:

``` terraform
  statement {
    actions   = ["s3:ListBucket"]
    resources = local.lamp_s3_buckets_readonly

    condition {
      test     = "StringEquals"
      variable = "s3:prefix"
      values   = [""]
    }
    condition {
      test     = "StringEquals"
      variable = "s3:delimiter"
      values   = ["/"]
    }
    sid = "AllowListingOfDataPlatformBuckets"
  }
```

This syntax makes it easy to control access to specific prefixes or
buckets but tedious to create exceptions within those prefixes or
buckets. To separate the PII we’re receiving currently—operator name,
Id, and run Id—we want to use a new prefix, `lamp-restricted` and
further separate data within that bucket by role. For instance, we may
want a role that can read operator IDs but not their names. To
accomplish that, we could change the directory structure of our
`springboard` buckets from:

    .
    └── lamp/
        ├── TM/
        │   ├── TMMAIN_OPERATOR
        │   ├── TMDailyLogStopCrossing
        │   ├── TMDailyLogDailyWorkPieces
        │   └── ...
        ├── BUS_VEHICLE_POSITIONS/
        │   └── year/
        │       └── month/
        │           └── day/
        │               └── yyyy-MM-dd.parquet
        └── ...

to one where the protected columns are stored separately from the
unprotected columns

    .
    ├── lamp/
    │   ├── TM/
    │   │   ├── TMDailyLogStopCrossing
    │   │   └── ...
    │   ├── BUS_VEHICLE_POSITIONS/
    │   │   └── year/
    │   │       └── month/
    │   │           └── day/
    │   │               └── yyyy-MM-dd.parquet
    │   └── ...
    └── lamp-restricted/
        ├── get-operator-id/
        │   ├── TMDailyLogDailyWorkPiece
        │   └── BUS_VEHICLE_POSITIONS/
        │       └── year/
        │           └── month/
        │               └── day/
        │                   └── yyyy-MM-dd.parquet
        └── get-operator-name/
            ├── TMMAIN_OPERATOR
            └── BUS_VEHICLE_POSITIONS/
                └── year/
                    └── month/
                        └── day/
                            └── yyyy-MM-dd.parquet

Obviously, this paradigm introduces complexity for readers and writers:
each level of access requires a different directory and child table. In
addition, readers need to join the separate columns back together to
reconstitute original records.

An alternative approach could use the same directory structures but
include full copies of all columns to make reading easier. That would
incur higher storage costs but reduce the friction of working with these
data.

Last, a third and final approach could protect individual columns.
Enterprise data catalogs such as AWS Athena implement column-level
access control on read but Parquet offers [a native implementation of
column-level
encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/).
Protected data would be stored in the same files as unprotected data but
the blocks of files that make up protected columns are encrypted using
keys specific to their policies. For instance, we could use a key for
operator Id and a separate key for operator name. Anyone with both keys
could view both fields.

[Uber implemented this
approach](https://www.uber.com/blog/one-stone-three-birds-finer-grained-encryption-apache-parquet/)
and it’s easy to write but readers would need to inject keys into their
reading applications. One barrier to this approach is that [DuckDB
currently only supports encryption at the
file-level](https://duckdb.org/docs/stable/data/parquet/encryption)
rather than column-level. As a result, any tables meant to be read by
DuckDB could only support a single level of access. Tables like
`TMMAIN_OPERATOR` would need to be encrypted with the key for the
highest level of access (eg. operator name) and would remain unavailable
to users who could see operator IDs.
