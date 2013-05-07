#!/usr/bin/perl

use strict;
use warnings;

#
# This is an example Perl Kafka program that can list the topics and number of partitions for a broker group.
#
my $USAGE = <<USAGE;
> ./topic_lister.pl --broker localhost:9092
Topics found:
* 'testTopic'
* 'BaconBacon'
* 'water-unicorns'

USAGE

# For getting topic metadata we only need a connection to the brokers
use Kafka::BrokerChannel; 

use Getopt::Long;
use Data::Dumper;

my @brokers = ();

GetOptions(
    "broker=s" => \@brokers, # Get broker list from the command line
);

if (scalar(@brokers) == 0) {
    @brokers = ("localhost:9092"); # default broker
}
my $broker_list = join(",", @brokers);
my $channel = Kafka::BrokerChannel->new(
    broker_list => $broker_list,
);

if (!defined($channel)) {
    die("Unable to connect to brokers: '$broker_list'");
}

my @topics = $channel->topic_list();

print "Topics found:\n";
foreach my $topic (@topics) {
    print "* '$topic'\n";
}

