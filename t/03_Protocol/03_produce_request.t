#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::produce_request_ng

use lib 'lib';

use Test::More tests => 23;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, "", "0", "0.5", "1", \"scalar", [] )

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw( produce_request_ng ) }

# -- declaration of variables to test
my $topic       = "test";
my $partition   = 0;
my $out         = undef;

# control request to a single message
my $single_message = [
        [undef, undef, "The first message"],
    ];
my $request_single =                            # PRODUCE Request - "no compression" now
    # Produce Request
     'ffff'                 # Required ACKS
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
        [undef, undef, "The first message"] ,
        [undef, undef, "The second message"],
        [undef, undef, "The third message"],
    ];
my $request_series =                            # PRODUCE Request - "no compression" now
    # Produce Request
     'ffff'                 # Required ACKS
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

# control request to a series of messages
my $multiple_partitions = {
    0 => [[undef, undef, "The first partition"]],
    1 => [[undef, undef, "The second partition"]],
};
my $partition_00 =
     '00000000'     # partition
    .'0000002d'     # Messageset size
    # Message Set
    # MESSAGE
    .'ffffffffffffffff'     # Offset
    .'00000021'             # Message size
    .'c457afbe'                                 # Checksum
    .'00'                                       # Magic
    .'00'                                       # Compression
    .'ffffffff'                                 # Key
    .'00000013'                                 # Length
    .'54686520666972737420706172746974696f6e'   # Payload ("The first message")
    ;
my $partition_01 =
     '00000001'     # partition
    .'0000002e'     # Messageset size
    # Message Set
    # MESSAGE
    .'ffffffffffffffff'     # Offset
    .'00000022'             # Message size
    .'ba515846'                                 # Checksum
    .'00'                                       # Magic
    .'00'                                       # Compression
    .'ffffffff'                                 # Key
    .'00000014'                                 # Length
    .'546865207365636f6e6420706172746974696f6e' # PAYLOAD ("The second message")
    ;
my $request_multi_partitions =                            # PRODUCE Request - "no compression" now
    # Produce Request
     'ffff'                 # Required ACKS
    .'000003e8'     # TIMEOUT
    .'00000001'     # Number of topics
    .'0004'     # Topic length
    .'74657374'     # Topic (test)
    .'00000002'     # number of partitions
# Note: the partition order may change...
    .$partition_00
    .$partition_01
    ;

# control request to a series of messages
my $multi_topics = {
        "test01" => $multiple_partitions,
        "test02" => $multiple_partitions,
    };
my $topic_01 =
     '0006'         # Topic length
    .'746573743031' # Topic (test)
    .'00000002'     # number of partitions
    .$partition_00
    .$partition_01
    ;
my $topic_02 =
     '0006'         # Topic length
    .'746573743032' # Topic (test)
    .'00000002'     # number of partitions
    .$partition_00
    .$partition_01
    ;
my $request_multi_topics_partitions =                            # PRODUCE Request - "no compression" now
    # Produce Request
     'ffff'                 # Required ACKS
    .'000003e8'     # TIMEOUT
    .'00000002'     # Number of topics
    .$topic_02
    .$topic_01
    ;





# INSTRUCTIONS -----------------------------------------------------------------

# -- verify response to invalid arguments

# without args
throws_ok { produce_request_ng() }                                             qr/^Mismatch argument/, 'expecting to die: Mismatch argument';

# bad types
foreach my $data ( (undef, [], ['hello world'], 0, "", "0" ) ) {
    throws_ok { produce_request_ng($data) }                                    qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    throws_ok { produce_request_ng({ $topic => { $partition => $single_message } }) }    qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# messages:
#   to see if a value is a normal non-false string of non-zero length
#   or a raw and unblessed ARRAY reference, allowing ARRAY references that contain no elements
foreach my $messages ( ( undef, 0, "", "0", \"scalar" ) )
{
    throws_ok { produce_request_ng( $topic, $partition, $messages ) }          qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# -- verify request form for a single message
$out = produce_request_ng({ $topic => { $partition => $single_message } });
is unpack( "H*", $$out), $request_single,   "correct request for a single message";

# -- verify request form for a series of messages
$out = produce_request_ng({ $topic => { $partition => $series_of_messages } });
is unpack( "H*", $$out), $request_series,   "correct request for a series of messages";

# -- verify request form for a set of partition and one topic
$out = produce_request_ng({ $topic => $multiple_partitions });
is unpack( "H*", $$out), $request_multi_partitions,   "correct request for multiple partitions";

# -- verify request form for a multiple topics and multiple partitions per topic
$out = produce_request_ng($multi_topics);
is unpack( "H*", $$out), $request_multi_topics_partitions,   "correct request for multiple topics and mutiple partitions";

# POSTCONDITIONS ---------------------------------------------------------------
