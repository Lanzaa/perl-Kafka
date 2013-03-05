#!/usr/bin/perl

use strict;
use warnings;

use Kafka qw(
    BITS64
    KAFKA_SERVER_PORT
    DEFAULT_TIMEOUT
    TIMESTAMP_EARLIEST
    TIMESTAMP_LATEST
    DEFAULT_MAX_OFFSETS
    DEFAULT_MAX_SIZE
    );

# common information
print "This is Kafka package $Kafka::VERSION\n";
print "You have a ", BITS64 ? "64" : "32", " bit system\n";

use Kafka::IO;
use Kafka::Consumer;

# connect to local server with the defaults
my $io = Kafka::IO->new( host => "localhost", port => 9093 );

# decoding of the error code
unless ( $io )
{
    print STDERR "last error: ",
        $Kafka::ERROR[Kafka::IO::last_errorcode], "\n";
}

#-- Consumer
my $consumer = Kafka::Consumer->new( IO => $io );

my $topic = "test04";

# Get a list of valid offsets up max_number before the given time
my $offsets;
my $hw = 0;
if ( $offsets = $consumer->offsets(
    $topic,             # topic
    0,                  # partition
    TIMESTAMP_LATEST, # time
    1, # max_number
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

# Consuming messages
while ( my $messages = $consumer->fetch(
    $topic,             # topic
    0,                  # partition
    $hw,                  # offset
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

$consumer->close;
