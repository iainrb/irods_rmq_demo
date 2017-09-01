#! usr/bin/env perl

use strict;
use warnings;
use Net::AMQP::RabbitMQ;

# set up a logging queue and bind to the gateway exchange

my $channel = 1;
my $exchange = 'npg.gateway';
my $queue = 'demo_irods_publish';

run() unless caller;

sub run {
    # 'main' method to run script
    my $rmq = Net::AMQP::RabbitMQ->new();
    my $args = {
	user     => 'demo_user',
	password => 'banquo',
        vhost    => '/demo',
	port     => 5672,

    };
    $rmq->connect('localhost', $args);
    $rmq->channel_open($channel);
    $rmq->exchange_declare(
			   $channel,
			   $exchange,
			   { exchange_type => 'fanout' },
			  );
    $rmq->queue_declare(
			$channel,
			$queue,
		       );
    $rmq->queue_bind(
		     $channel,
		     $queue,
		     $exchange,
		     'test.irods.*',
		    );
    $rmq->disconnect();

  }

1;
