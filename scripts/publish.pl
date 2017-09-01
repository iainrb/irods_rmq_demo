#! usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use Getopt::Long;
use Log::Log4perl qw(:levels);

use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);

my $irods_class      = 'WTSI::NPG::TestMQiRODS';
my $publisher_class  = 'WTSI::NPG::TestMQPublisher';

eval "require $irods_class";
eval "require $publisher_class";

$irods_class->import;
$publisher_class->import;

my $channel = 1;
my $test_host = 'localhost';
my $port = 5672;
my $rkey_prefix = 'test';
my $session_log = '/tmp/rmq_demo_publish.log';

run() unless caller;

sub run {

  # 'main' method to run script
  # parse command-line options
  my $prefix;
  my $exchange;
  my $config; # RabbitMQ JSON config
  my $password;
  my $total; # total files to publish
  my $dest; # destination root collection
  my $log4perl_config;

  GetOptions(
	     'config=s'   => \$config,
	     'prefix=s'   => \$prefix,
	     'exchange=s' => \$exchange,
	     'total=i'    => \$total,
	     'dest=s'     => \$dest,
	     'logconf=s'  => \$log4perl_config,
	    );
  $prefix ||= 'hamlet';
  $exchange ||= 'npg.gateway';
  $total ||= 5;
  $dest ||= '/tempZone/home/ubuntu/demo';
  $config ||= "$Bin/../etc/demo_config.json";
  $log4perl_config ||= 'etc/log4perl.conf';

  my @log_levels = ($DEBUG, );
  log_init(config => $log4perl_config,
           file   => $session_log,
           levels => \@log_levels);
  my $log = Log::Log4perl->get_logger('main');

  my $irods = $irods_class->new(environment          => \%ENV,
				strict_baton_version => 0,
				exchange             => $exchange,
				routing_key_prefix   => $rkey_prefix,
				hostname             => $test_host,
				rmq_config_path      => $config,
				channel              => $channel,
			       );
  $irods->rmq_init();

  my $publisher = $publisher_class->new(
					irods                => $irods,
					exchange             => $exchange,
					routing_key_prefix   => $rkey_prefix,
					hostname             => $test_host,
					rmq_config_path      => $config,
					channel              => $channel,
				       );
  $publisher->rmq_init();

  my $i = 1;

  while ($i <= $total) {
    my $filename = "hamlet.$i";
    my $source = "$Bin/../data/hamlet_split/$filename";
    my $sub_coll = $irods->hash_path($source);
    my $remote_path = "$dest/$sub_coll/$filename";
    if ($irods->is_object($remote_path)) {
      $log->info("Omitting publication of $remote_path: Already exists");
    } else {
      $log->info("Publishing $remote_path to iRODS");
      $publisher->publish($source, $remote_path);
      $irods->add_object_avu($remote_path, 'block_number', $i);
      $irods->add_object_avu($remote_path, 'wrong_number', $i+10000);
      $irods->remove_object_avu($remote_path, 'wrong_number', $i+10000);
    }
    $i++;
  }

  $irods->rmq_disconnect();
  $publisher->rmq_disconnect();

}

1;

