## Overview
```Flare-up``` provides a wrapper around the Redshift commands [```CREATE TABLE```](http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html), [```COPY```](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html), and [```DROP TABLE```](http://docs.aws.amazon.com/redshift/latest/dg/r_DROP_TABLE.html)  for scriptability, allowing you to issue the commands directly from the CLI.  Much of the code is concerned with simplifying constructing the COPY command and providing easy access to the errors that may result from import.

## Why?

Redshift prefers a bulk COPY operation over indidivual INSERTs which Redshift is not optimized for, and Amazon does not recommend it as a strategy for loading.  COPY is a SQL command, not something issued via the AWS Redshift REST API, meaning you need a SQL connection to your Redshift instance to bulk load data.

The astute consumer of the AWS toolchain will note that [Data Pipeline](http://aws.amazon.com/datapipeline/) is one way this import may be completed however, we use Azkaban and the only thing worse than one job flow control tool is two job flow control tools :)

Additionally, access to COPY errors is a bit cumbersome.  On failure, Redshift populates the ```stl_load_errors``` table which inherently must be accessed via SQL.  Flare-up will pretty print any errors that occur during import so that you may examine your logs rather than establishing a connection to Redshift to understand what went wrong.

## Requirements and Installation

The `pg` gem is a dependency (required to issue SQL commands to Redshift) and will be pulled down with flare-up.

```
> gem install flare-up
```

## Syntax

Available via `flare-up help <cmd>` where `<cmd>` can be replaced with `create_table`, `copy`, or `drop_table`.

While we'd prefer if everyone stored configuration variables (esp. credentials) as environment variables (re: [Twelve-Factor App](http://12factor.net/)), it can be a pain to export variables when you're testing a tool and as such, we support specifying all of these on the command-line.

### COPY

```
Usage:
  flare-up copy DATA_SOURCE REDSHIFT_ENDPOINT DATABASE TABLE

Options:
  [--aws-access-key=AWS_ACCESS_KEY]            # Required unless ENV['AWS_ACCESS_KEY_ID'] is set.
  [--aws-secret-key=AWS_SECRET_KEY]            # Required unless ENV['AWS_SECRET_ACCESS_KEY'] is set.
  [--redshift-username=REDSHIFT_USERNAME]      # Required unless ENV['REDSHIFT_USERNAME'] is set.
  [--redshift-password=REDSHIFT_PASSWORD]      # Required unless ENV['REDSHIFT_PASSWORD'] is set.
  [--redshift-port=REDSHIFT_PORT]              # Optional (defaults to 5439)
  [--column-list=one two three]                # A space-separated list of columns, should your DATA_SOURCE require it
  [--copy-options=COPY_OPTIONS]                # Appended to the end of the COPY command; enclose "IN QUOTES"
  [--colorize-output], [--no-colorize-output]  # Should Flare-up colorize its output?
                                               # Default: true
```

### CREATE TABLE

```
Usage:
  flare-up create_table REDSHIFT_ENDPOINT DATABASE TABLE

Options:
  [--column-list=COLUMN_LIST]                  # Required. A space-separated list of columns with their data-types, enclose "IN QUOTES"
  [--redshift-username=REDSHIFT_USERNAME]      # Required unless ENV['REDSHIFT_USERNAME'] is set.
  [--redshift-password=REDSHIFT_PASSWORD]      # Required unless ENV['REDSHIFT_PASSWORD'] is set.
  [--redshift-port=REDSHIFT_PORT]              # Optional (defaults to 5439)
  [--colorize-output], [--no-colorize-output]  # Should Flare-up colorize its output?
                                               # Default: true
```

### DROP TABLE

```
Usage:
  flare-up drop_table REDSHIFT_ENDPOINT DATABASE TABLE

Options:
  [--redshift-username=REDSHIFT_USERNAME]      # Required unless ENV['REDSHIFT_USERNAME'] is set.
  [--redshift-password=REDSHIFT_PASSWORD]      # Required unless ENV['REDSHIFT_PASSWORD'] is set.
  [--redshift-port=REDSHIFT_PORT]              # Optional (defaults to 5439)
  [--colorize-output], [--no-colorize-output]  # Should Flare-up colorize its output?
                                               # Default: true
```

### TRUNCATE

```
Usage:
  flare-up truncate REDSHIFT_ENDPOINT DATABASE TABLE

Options:
  [--redshift-username=REDSHIFT_USERNAME]      # Required unless ENV['REDSHIFT_USERNAME'] is set.
  [--redshift-password=REDSHIFT_PASSWORD]      # Required unless ENV['REDSHIFT_PASSWORD'] is set.
  [--redshift-port=REDSHIFT_PORT]              # Optional (defaults to 5439)
  [--colorize-output], [--no-colorize-output]  # Should Flare-up colorize its output?
                                               # Default: true
```

## Sample Usage

Note that these examples assume you have credentials set as environment variables.

### CREATE TABLE

```
> flare-up                                                      \
    create_table                                                \
    flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
    dev                                                         \
    hearthstone_cards                                           \
    --column-list "id char(24) name varchar(2000)"
```

### COPY

```
> flare-up                                                      \
    copy                                                        \
    s3://slif-redshift/hearthstone_cards_short_list.csv         \
    flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
    dev                                                         \
    hearthstone_cards                                           \
    --column-list name cost attack health description           \
    --copy-options "REGION 'us-east-1' CSV IGNOREHEADER 1"
```

- The handy `IGNOREHEADER 1` option ignores the first line of field names  in the csv file.

### DROP TABLE

```
> flare-up                                                      \
    drop_table                                                  \
    flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
    dev                                                         \
    hearthstone_cards
```

### TRUNCATE

```
> flare-up                                                      \
    truncate                                                    \
    flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
    dev                                                         \
    hearthstone_cards
```

## License

This project is Copyright (c) 2015, Sharethrough and licensed under the [MIT License](LICENSE.md).
