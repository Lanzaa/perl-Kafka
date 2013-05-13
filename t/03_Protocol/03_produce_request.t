#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::produce_request

use lib 'lib';

use Test::More tests => 22;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, "", "0", "0.5", "1", \"scalar", [] )

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw( produce_request ) }

# -- declaration of variables to test
my $topic       = "test";
my $partition   = 0;

# control request to a single message
my $single_message = "The first message";
my $request_single =                            # PRODUCE Request - "no compression" now
    # Request Header
     '0000005b'                                 # REQUEST_LENGTH
    .'0000'                 # API KEY
    .'0000'                 # API VERSION
    .'ffffffff'                 # CORRELATION ID
    .'000a'                     # CLient id length
    .'7065726c2d6b61666b61'     # CLIENT ID (perl-kafka)
    # Produce Request
    .'ffff'                 # Required ACKS
    .'000003e8'     # TIMEOUT
    .'00000001'     # Number of topics
    .'0004'     # Topic length
    .'74657374'     # Topic (test)
    .'00000001'     # number of partitions
    .'00000000'     # partition
    .'0000002b'     # Messageset size
    # Message Set
    .'ffffffffffffffff' # Offset (-1)
    .'0000001f'         # Message Size
    # Message
    .'dd1e9a39'                                 # Checksum
    .'00'                                       # Magic
    .'00'                                         # Attributes
    .'ffffffff'                                 # Key size
    .'00000011'                                 # Payload Size
    .'546865206669727374206d657373616765'       # Payload ("The first message")
    ;

# control request to a series of messages
my $series_of_messages = [
        "The first message",
        "The second message",
        "The third message",
    ];
my $request_series =                            # PRODUCE Request - "no compression" now
    # Request Header
     '000000b2'                                 # REQUEST_LENGTH
    .'0000'                 # API KEY
    .'0000'                 # API VERSION
    .'ffffffff'                 # CORRELATION ID
    .'000a'                     # CLient id length
    .'7065726c2d6b61666b61'     # CLIENT ID (perl-kafka)
    # Produce Request
    .'ffff'                 # Required ACKS
    .'000003e8'     # TIMEOUT
    .'00000001'     # Number of topics
    .'0004'     # Topic length
    .'74657374'     # Topic (test)
    .'00000001'     # number of partitions
    .'00000000'     # partition
    .'00000082'     # Messageset size
    # Message Set
    # MESSAGE
    .'ffffffffffffffff'     # Offset
    .'0000001f'             # Message size
    .'dd1e9a39'                                 # Checksum
    .'00'                                       # Magic
    .'00'                                       # Compression
    .'ffffffff'                                 # Key
    .'00000011'                                 # Length
    .'546865206669727374206d657373616765'       # Payload ("The first message")
    # MESSAGE
    .'ffffffffffffffff'     # Offset
    .'00000020'             # Message size
    .'730fccd7'                                 # Checksum
    .'00'                                       # Magic
    .'00'                                       # Compression
    .'ffffffff'                                 # Key
    .'00000012'                                 # Length
    .'546865207365636f6e64206d657373616765'     # PAYLOAD ("The second message")
    # MESSAGE
    .'ffffffffffffffff'     # Offset
    .'0000001f'             # Message size
    .'5c35af07'                                 # Checksum
    .'00'                                       # Magic
    .'00'                                       # Compression
    .'ffffffff'                                 # Key
    .'00000011'                                 # Length
    .'546865207468697264206d657373616765'       # PAYLOAD ("The third message")
    ;

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify response to invalid arguments

# without args
throws_ok { produce_request() }                                             qr/^Mismatch argument/, 'expecting to die: Mismatch argument';

# topic: to see if a value is a normal non-false string of non-zero length
foreach my $topic ( ( undef, 0, "", "0", \"scalar", [] ) )
{
    throws_ok { produce_request( $topic, $partition, $single_message ) }    qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    throws_ok { produce_request( $topic, $partition, $single_message ) }    qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# messages:
#   to see if a value is a normal non-false string of non-zero length
#   or a raw and unblessed ARRAY reference, allowing ARRAY references that contain no elements
foreach my $messages ( ( undef, 0, "", "0", \"scalar" ) )
{
    throws_ok { produce_request( $topic, $partition, $messages ) }          qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# -- verify request form for a single message
is unpack( "H*", produce_request( $topic, $partition, $single_message       ) ), $request_single,   "correct request for a single message";

# -- verify request form for a series of messages
is unpack( "H*", produce_request( $topic, $partition, $series_of_messages   ) ), $request_series,   "correct request for a series of messages";

# POSTCONDITIONS ---------------------------------------------------------------
