#!/usr/bin/perl -I ../lib/ -I lib/

use strict;
use warnings;

use Kafka::Producer;

use Getopt::Long;
use Data::Dumper;

sub usage {
    my $exit = shift;
    print <<USAGE;
> ./console-producer.pl --broker localhost:9092 --topic testing03
USAGE
    return $exit;
}

my @brokers = ();
my $topic = "";
GetOptions(
    "broker=s" => \@brokers,
    "topic=s" => \$topic,
);

# Usage check
if (scalar(@brokers) == 0 || length($topic) == 0) {
    exit(usage(1));
}

#-- Producer
my $producer = Kafka::Producer->new( 
    broker_list => join(",", @brokers),
);

while (<STDIN>) {
    chomp $_;
    my $data = $_;

    print "Sending message: '$data'\n";
    my $ret = $producer->sendMsg($topic, $data);
}

$producer->close();
