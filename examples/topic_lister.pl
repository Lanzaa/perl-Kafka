#!/usr/bin/perl -I ../lib -I lib

use strict;
use warnings;

#
# This is an example Perl Kafka program that can list the topics and number of partitions for a broker group.
#
my $USAGE = <<USAGE;
Example usage:
> ./topic_lister.pl --broker localhost:9092 [--broker localhost:9093 ...] [--debug]

USAGE

my $EXAMPLE_OUTPUT = <<OUTPUT;
> ./topic_lister.pl --broker localhost:9092 [--broker localhost:9093 ...] [--debug]
Topics found:
* 'testTopic'
* 'BaconBacon'
* 'water-unicorns'

OUTPUT

# For getting topic metadata we only need a connection to the brokers
use Kafka::BrokerChannel;

use Getopt::Long;
use Data::Dumper;

my @brokers = ();
my $debug = 0;

GetOptions(
    "broker=s" => \@brokers, # Get broker list from the command line
    "debug", \$debug,
);

if (scalar(@brokers) == 0) {
    print $USAGE;
    exit(1);
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
    if ($debug) {
        # Print detailed information about each topic.
        my $topic_metadata = $channel->getTopicMetadata($topic);
        my @partitions = keys $topic_metadata;
        foreach my $partition (sort @partitions) {
            my $data = $topic_metadata->{$partition};
            printf("\tpartition: %d\tleader: %d\tisr: %s\treplicas: %s\terror: %d\n",
                $partition,
                $data->{leader},
                "[".join(", ", @{$data->{isr}})."]",
                "[".join(", ", @{$data->{replicas}})."]",
                $data->{error_code}
            );
        }
    }
}

