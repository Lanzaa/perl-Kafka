#!/usr/bin/perl -I ../lib/ -I lib/

use strict;
use warnings;

use Kafka qw(
    TIMESTAMP_EARLIEST
    TIMESTAMP_LATEST
    DEFAULT_MAX_OFFSETS
    DEFAULT_MAX_SIZE
    );

use Kafka::Consumer;

use Getopt::Long;
use Data::Dumper;

sub usage {
    my $exit = shift;
    print <<USAGE;
> ./console-consumer.pl --broker localhost:9092 --topic testing03 --partition 0 [--offset 10]
USAGE
    return $exit;
}

my @brokers = ();
my $topic = "";
my $partition = undef;
my $hw = undef;
GetOptions(
    "broker=s" => \@brokers,
    "topic=s" => \$topic,
    "partition=i" => \$partition,
    "offset=i" => \$hw,
);

# Usage check
if (!defined($partition) || scalar(@brokers) == 0 || length($topic) == 0) {
    exit(usage(1));
}

#-- Consumer
my $consumer = Kafka::Consumer->new(
    broker_list => join(",", @brokers),
    RaiseError => 1,
);

unless(defined($consumer)) { warn("No consumer!"); }

# Get a list of valid offsets up max_number before the given time
if (!defined($hw)) {
    $hw = 0;
    my $offsets;
    if ( $offsets = $consumer->offsets(
        $topic,             # topic
        $partition,         # partition
        TIMESTAMP_LATEST, # time
        DEFAULT_MAX_OFFSETS, # max_number
        ) )
    {
        foreach my $offset ( @$offsets )
        {
            print "Received offset: $offset\n";
            if ($offset > $hw) {
                $hw = $offset;
            }
        }
    }
    if ( !$offsets or $consumer->last_error )
    {
        print
            "(", $consumer->last_errorcode, ") ",
            $consumer->last_error, "\n";
    }
}

print "Beginning to fetch\n";

# Consuming messages
while ( my $messages = $consumer->fetch(
    $topic,             # topic
    $partition,         # partition
    $hw,                # offset
    DEFAULT_MAX_SIZE    # Maximum size of MESSAGE(s) to receive
    ) )
{
    foreach my $message ( @$messages )
    {
        if( $message->valid )
        {
            print "payload    : ", $message->payload,       "\n";
            print "offset     : ", $message->offset,        "\n";
            print "next_offset: ", $message->next_offset,   "\n";
            $hw = $message->next_offset;
        }
        else
        {
            print "error      : ", $message->error,         "\n";
        }
    }
    sleep 3;
}

print "Dump consumer: ".Dumper(\$consumer);

$consumer->close();
