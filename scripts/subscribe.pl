#! usr/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use FindBin qw($Bin);

use WTSI::NPG::RabbitMQ::TestCommunicator;

my $channel = 1;
my $hostname = 'localhost';
my $conf = "$Bin/../etc/demo_config.json";
my $queue = 'demo_irods_publish';

run() unless caller;

sub run {


  my $args = {
	      hostname             => $hostname, # global variable
	      rmq_config_path      => $conf,      # global variable
	      channel              => $channel,
	     };

  my $communicator = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);

  my @messages = $communicator->read_all($queue);

  foreach my $message (@messages) {
    print Dumper $message;
  }

}

1;
