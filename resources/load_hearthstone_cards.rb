require 'flare_up'

host_name = 'flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com'
db_name = 'dev'
table_name = 'hearthstone_cards'
data_source = 's3://slif-redshift/hearthstone_cards_short_list.csv'

conn = FlareUp::Connection.new(host_name, db_name, ENV['REDSHIFT_USERNAME'], ENV['REDSHIFT_PASSWORD'], ENV['REDSHIFT_PORT'])

copy = FlareUp::Command::Copy.new(table_name, data_source, ENV['AWS_ACCESS_KEY_ID'], ENV['AWS_SECRET_ACCESS_KEY'])
copy.columns = %w(name cost attack health description)
copy.options = "REGION 'us-east-1' CSV"

result = copy.execute(conn)

good_command = <<-COMMAND
flare-up copy \
  s3://slif-redshift/hearthstone_cards_short_list.csv \
  flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
  dev \
  hearthstone_cards \
  --column-list name cost attack health description \
  --copy_options "REGION 'us-east-1' CSV"
COMMAND

bad_data_source_command = <<-COMMAND
flare-up copy \
  s3://slif-redshift/hearthstone_cards_short_lissdsdsdsdsdsdsd.csv \
  flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
  dev \
  hearthstone_cards \
  --column-list name cost attack health description \
  --copy_options "REGION 'us-east-1' CSV"
COMMAND

bad_data_command = <<-COMMAND
flare-up copy \
  s3://slif-redshift/hearthstone_cards_broken.csv \
  flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
  dev \
  hearthstone_cards \
  --column-list name cost attack health description \
  --copy_options "REGION 'us-east-1' CSV"
COMMAND

other_zone_bucket = <<-COMMAND
flare-up copy \
  s3://slif-redshift/hearthstone_cards_short_list.csv \
  flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
  dev \
  hearthstone_cards \
  --column-list name cost attack health description \
  --copy_options CSV
COMMAND

busted_options = <<-COMMAND
flare-up copy \
  s3://slif-redshift/hearthstone_cards_short_list.csv \
  flare-up-test.cskjnp4xvaje.us-west-2.redshift.amazonaws.com \
  dev \
  hearthstone_cards \
  --column-list name cost attack health description \
  --copy_options "CSV ;lmlkmlk3"
COMMAND
