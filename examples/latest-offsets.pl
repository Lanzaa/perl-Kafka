#!/usr/bin/perl -I ../lib -I lib

use strict;
use warnings;

#
# This is an example Perl Kafka program that can list the latest available offsets for a topic.
#
my $USAGE = <<USAGE;
Example usage:
> ./latest-offsets.pl --broker localhost:9092 [--broker localhost:9093 ...] --topic test [--topic blah ...]

USAGE

# For getting topic metadata we only need a connection to the brokers
use Kafka qw( TIMESTAMP_LATEST );
use Kafka::BrokerChannel;
use Kafka::Consumer;

use Getopt::Long;
use Data::Dumper;

my @brokers = ();
my @topics = ();
my $debug = 0;

GetOptions(
    "broker=s" => \@brokers, # Get broker list from the command line
    "topic=s" => \@topics,
    "debug", \$debug,
);

if (scalar(@brokers) == 0 || scalar(@topics) == 0) {
    print $USAGE;
    exit(1);
}
my $broker_list = join(",", @brokers);
my $channel = Kafka::BrokerChannel->new(
    broker_list => $broker_list,
);
my $consumer = Kafka::Consumer->new(
    BC => $channel,
);

if (!defined($consumer)) {
    die("Unable to connect to brokers: '$broker_list'");
}

foreach my $topic (@topics) {
    my $topic_metadata = $channel->getTopicMetadata($topic);
    my @partitions = keys $topic_metadata;
    foreach my $partition (sort @partitions) {
        my $offsets = $consumer->offsets(
            $topic,
            $partition,
            TIMESTAMP_LATEST,
            5,
        );
        print "topic: $topic\tpartition: $partition\toffsets: ".join(", ",@$offsets)."\n";
    }
}


