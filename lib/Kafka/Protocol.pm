package Kafka::Protocol;

use 5.010;
use strict;
use warnings;

use Exporter qw( import );
our @EXPORT_OK  = qw(
    REQUESTTYPE_PRODUCE
    REQUESTTYPE_FETCH
    REQUESTTYPE_MULTIFETCH
    REQUESTTYPE_MULTIPRODUCE
    REQUESTTYPE_OFFSETS
    metadata_request
    metadata_request_ng
    metadata_response
    metadata_response_ng
    produce_request
    produce_request_ng
    produce_response
    produce_response_ng
    fetch_request
    fetch_request_ng
    offsets_request
    offsets_request_ng
    fetch_response
    fetch_response_ng
    offsets_response
    offsets_response_ng
    request_header_encode
    APIKEY_METADATA
    APIKEY_PRODUCE
    APIKEY_OFFSETS
    APIKEY_FETCH
    CLIENT_ID
    );

our $VERSION = '0.22';

use bytes;
use Carp;
use Digest::CRC     qw( crc32 );
use Params::Util    qw( _STRING _NONNEGINT _POSINT _NUMBER _ARRAY0 _SCALAR _HASHLIKE );
use IO::Uncompress::Gunzip qw{ gunzip };

use Data::Dumper;

use Kafka qw(
    ERROR_INVALID_MESSAGE_CODE
    ERROR_MISMATCH_ARGUMENT
    ERROR_CHECKSUM_ERROR
    ERROR_COMPRESSED_PAYLOAD
    ERROR_NUMBER_OF_OFFSETS
    DEFAULT_TIMEOUT_MS
    BITS64
    );

if ( !BITS64 ) { eval 'use Kafka::Int64; 1;' or die "Cannot load Kafka::Int64 : $@"; }  ## no critic

use constant {
    DEBUG                               => 0,

    REQUESTTYPE_PRODUCE                 => 0,
    REQUESTTYPE_FETCH                   => 1,
    REQUESTTYPE_MULTIFETCH              => 2,   # Not used now
    REQUESTTYPE_MULTIPRODUCE            => 3,   # Not used now
    REQUESTTYPE_OFFSETS                 => 4,

    MAGICVALUE_NOCOMPRESSION            => 0,
    MAGICVALUE_COMPRESSION              => 1,   # Not used now
    COMPRESSION_NO_COMPRESSION          => 0,
    COMPRESSION_GZIP                    => 1,   # Not used now
    COMPRESSION_SNAPPY                  => 2,   # Not used now

    APIKEY_PRODUCE                      => 0,
    APIKEY_FETCH                        => 1,
    APIKEY_OFFSETS                      => 2,
    APIKEY_METADATA                     => 3,
    APIKEY_LEADERANDIDR                 => 4,
    APIKEY_STOPREPLICA                  => 5,
    APIKEY_OFFSETCOMMIT                 => 6,
    APIKEY_OFFSETFETCH                  => 7,

    ACK_NONE                            => 0,
    ACK_LOCAL                           => 1,
    ACK_ALLREPLICAS                     => -1,

    REPLICAID                           => -1,
    SENTINEL_OFFSET                     => -1, # Can be anything

    # TODO These probably should be user setable
    CLIENT_ID                           => "perl-kafka",
    MAX_WAIT_TIME                       => 2,
    MIN_BYTES                           => 1,
};

our $_last_error;
our $_last_errorcode;

my $position;

##
# Used to unpack stuff and automattically put in various.
#
# Expects:
#   * a pack string
#   * a response hash
#   * number of bytes to move the response forward
# Returns:
#   the unpacked data
##
sub unpackRes {
    my ($pack_string, $response) = @_;
    return unpack("x$response->{position} ".$pack_string, $response->{data});
}

##
# Used to unpack a 64 bit number. Ex an offset.
##
sub unpackRes64 {
    my $response = shift;
    return ( BITS64
        ? unpack("x$response->{position} q>", $response->{data} )
        : Kafka::Int64::unpackq(unpack("x$response->{position} a8", $response->{data}))
    );
}

##
# Used to pack a 64 bit number. Ex an offset.
##
sub pack64 {
    my $number = shift;
    return ( BITS64 
        ? pack( "q>", $number + 0 ) 
        : Kafka::Int64::packq( $number ) 
    );   # OFFSET
}

sub last_error {
    return $_last_error;
}

sub last_errorcode {
    return $_last_errorcode;
}

sub _error {
    my $error_code  = shift;

    $_last_errorcode  = $error_code;
    $_last_error      = $Kafka::ERROR[$_last_errorcode];
    confess $_last_error;
}

# Wire Format: https://cwiki.apache.org/confluence/display/KAFKA/Wire+Format
################################################################################
# Requests Wire Format

# Request Header ---------------------------------------------------------------

sub _request_header_encode {
    my $request_length  = shift;
    my $request_type    = shift;
    my $client_id       = shift;
    my $correlation_id  = shift || -1; #XXX TODO
    my $api = 0;

    my $encoded = pack("
        s>      # ApiKey
        s>      # ApiVersion
        l>      # Correlation id
        s>/a   # length of client_id string
        ",
        $request_type,
        $api,
        $correlation_id,
        $client_id,
    );

    my $len = $request_length + bytes::length($encoded);

    if ( DEBUG )
    {
        print STDERR "Request header:\n"
            ."REQUEST_LENGTH    = ".($len)."\n"
            ."REQUEST_TYPE      = $request_type\n"
            ."CLIENT_ID         = $client_id\n"
            ."CORR_ID           = $correlation_id\n"
            ."API Version       = $api\n"
        ;
    }

    return pack("l>", $len).$encoded;
}

##
# Used to create a header for a Kafka request
#
# Expects:
#   * request length - the length of the request
#   * apikey - the apikey for the type of request
#   * clientId - the clients id
#   * correlationId - correlation id for identifying the response
# Returns:
#   the packed header
##
sub request_header_encode {
    my $request_length  = shift;
    my $request_type    = shift;
    my $client_id       = shift;
    my $correlation_id  = shift;
    my $api = 0;

    my $encoded = pack("
        s>      # ApiKey
        s>      # ApiVersion
        l>      # Correlation id
        s>/a   # length of client_id string
        ",
        $request_type,
        $api,
        $correlation_id,
        $client_id,
    );

    my $len = $request_length + bytes::length($encoded);

    if ( DEBUG )
    {
        print STDERR "Request header:\n"
            ."REQUEST_LENGTH    = ".($len)."\n"
            ."REQUEST_TYPE      = $request_type\n"
            ."CLIENT_ID         = $client_id\n"
            ."CORR_ID           = $correlation_id\n"
            ."API Version       = $api\n"
        ;
    }

    return pack("l>", $len).$encoded;
}

# METADATA Request

# TODO
# expects a string or array of strings for the topic
sub metadata_request {
    my $topics           = shift;

    (
        _STRING( $topics ) or
        _ARRAY0( $topics )
    ) or return _error( ERROR_MISMATCH_ARGUMENT );

    $topics = [ $topics ] if ( !ref( $topics ) );

    # TODO Allow ack level to be set
    # TODO Allow timeout to be set
    my $encoded = pack("
            l>          # Number of topics
        ",
            scalar(@$topics)
         );

    foreach my $topic (@$topics) {
        $encoded .= pack("s>/a", $topic);
    }

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            APIKEY_METADATA,
            CLIENT_ID,
            ).$encoded;

    return $encoded;
}

##
# Used to encode topics into a metadata request
#
# Returns:
#   a reference to encoded data
##
# TODO docs
sub metadata_request_ng {
    my $topics = shift;

    (
        _STRING( $topics ) or
        _ARRAY0( $topics )
    ) or return _error( ERROR_MISMATCH_ARGUMENT );

    $topics = [ $topics ] if ( !ref( $topics ) );

    my $encoded = pack("
            l>          # Number of topics
        ",
            scalar(@$topics)
         );

    foreach my $topic (@$topics) {
        $encoded .= pack("s>/a", $topic);
    }

    return \$encoded;
}





# PRODUCE Request --------------------------------------------------------------

##
# Used to pack a bunch of data to send in a produce request to a single broker.
#
# Expects:
#   * a hash of the data to pack: {
#       topic01 => {
#           part01 => [[topic01, key, 'data11'], [topic01, key, 'data12']],
#           part02 => [[topic01, key, 'data21']],
#           },
#       topic02 => { ... },
#    }
#   * (Optional) a hash of options. TODO list options
#       - ack_level     XXX not implemented
#       - timeout       XXX not implemented
#       - compression   XXX not implemented
# Returns:
#   * a reference to the packed data
##
# TODO impl, test, doc
sub produce_request_ng {
    my ($data, $opts) = @_;
    if (!defined(_HASHLIKE($data)) 
        || (defined($opts) && !defined(_HASHLIKE($opts)))) {
        return _error( ERROR_MISMATCH_ARGUMENT );
    }

    # TODO Allow ack level to be set
    # TODO Allow timeout to be set
    # TODO Allow compression to be set
    my $compression = 0;
    my $packed = pack("
            s>          # Required Acks
            l>          # Timeout
            l>          # Number of topics
            ", ACK_ALLREPLICAS, DEFAULT_TIMEOUT_MS, scalar(keys $data)
        );
    while (my ($topic, $tdata) = each($data)) {
        if (!defined(_STRING($topic)) 
            || !defined(_HASHLIKE($tdata))) {
            return _error( ERROR_MISMATCH_ARGUMENT );
        }
        
        $packed .= pack("
            s>/a    # Topic
            l>      # Number of partitions
            ", $topic, scalar(keys $tdata)
        );
        while (my ($partId, $pdata) = each($tdata)) {
            if (!defined(_NONNEGINT($partId))
                || !defined(_ARRAY0($pdata))) {
                return _error( ERROR_MISMATCH_ARGUMENT );
            }
            $packed .= pack("
                l>      # Partition id
                l>/a    # Message set length and data
                ", $partId, ${_messageset_encode_ng($pdata, { compression => $compression })}
            );
        }
    }
    return \$packed;
}

# LIMITATION: For all messages use the same properties:
#   magic:          Magic Value
#   compression:    (only for magic = MAGICVALUE_COMPRESSION)
# $messages may be one of:
#   simple SCALAR
#   reference to an array of scalars

sub produce_request {
    confess("[BUG] This should be removed.");
    my $topic           = _STRING( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    my $partition       = shift;
    my $messages        = shift;

    return _error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );

    (
        _STRING( $messages ) or
        _ARRAY0( $messages )
    ) or return _error( ERROR_MISMATCH_ARGUMENT );

    $messages = [ $messages ] if ( !ref( $messages ) );

    # TODO Allow ack level to be set
    # TODO Allow timeout to be set
    my $encoded = pack("
            s>          # Required Acks
            l>          # Timeout
            ", ACK_ALLREPLICAS, DEFAULT_TIMEOUT_MS,
        );

    # TODO Allow multiple topics and partitions
    my $messageset = _messageset_encode( $messages );
    $encoded .= pack("
        l>          # Number of topics
        s>/a        # Topic
        l>          # Number of partitions
        l>          # Partition
        l>/a        # Message Set len and data
        ",
        1,
        $topic,
        1,
        $partition,
        $messageset,
    );

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            APIKEY_PRODUCE,
            CLIENT_ID,
            ).$encoded;

    if ( DEBUG )
    {
        print STDERR "Produce request:\n"
            ."MESSAGES_LENGTH    = ".bytes::length( $encoded )."\n";
    }

    return $encoded;
}

# MESSAGE ----------------------------------------------------------------------

##
# Used to pack an array of messages.
#
# Expects:
#   * an array of messages:
#       [[topic, key, data], [topic, key, data]]
#   * (Optional) a hash of options
#       - compression   TODO XXX Support compression
# Returns:
#   a reference to the packed message set
##
# TODO support compression
sub _messageset_encode_ng {
    my ($messages, $opts) = @_;
    my $encoded = "";
    foreach my $msg ( @$messages )
    {
        my $encoded_message = pack( "
                c       # Magic
                c       # Attributes
                ", MAGICVALUE_NOCOMPRESSION, COMPRESSION_NO_COMPRESSION
            )
            .( defined($msg->[1]) ? pack("l>/a", $msg->[1]) : pack("l>", -1) ) # Pack the key
            .( defined($msg->[2]) ? pack("l>/a", $msg->[2]) : pack("l>", -1) ) # Pack the value
            ;
        $encoded .= ( BITS64
                    ? pack( "q>", SENTINEL_OFFSET )
                    : Kafka::Int64::packq( SENTINEL_OFFSET ) )   # OFFSET
            .pack("l> L> a* ",
                4+bytes::length($encoded_message),
                crc32($encoded_message),
                $encoded_message
            );
    }
    return \$encoded;
}

##
# Expects an array of messages and returns a wire encoded messageset
# Assumes all messages are for the same topic-partition
##
sub _messageset_encode {
    my $messages    = shift;
# for future versions
    my $magic       = shift || MAGICVALUE_NOCOMPRESSION;
    my $compression = shift || COMPRESSION_NO_COMPRESSION;

    my $encoded = "";
    foreach my $message ( @$messages )
    {
        return _error( ERROR_INVALID_MESSAGE_CODE ) if ref( $message );

        my $attributes = $compression;
        my $key; # The key used to decide which partition the message is sent to

        my $encoded_message = pack( "
                c       # Magic
                c       # Attributes
                ",
                $magic,
                $attributes,
            )
            .( $key ? pack("l>/a", $key) : pack("l>", -1) )
            .( $message ? pack("l>/a", $message) : pack("l>", -1) )
            ;
        $encoded .= ( BITS64
                    ? pack( "q>", SENTINEL_OFFSET )
                    : Kafka::Int64::packq( SENTINEL_OFFSET ) )   # OFFSET
            .pack("l> L> a* ",
                4+bytes::length($encoded_message),
                crc32($encoded_message),
                $encoded_message
            );
    }

    return $encoded;
}

##
# Used to decode a messageset from a 'buffer' object.
#
# Expects:
#   * buffer - contains the data and current position in the buffer
#       { 
#         position => 213,
#         data => '...',
#       }
#   * length - expected length of the message set
##
# TODO: rework to new style
# Note may not work if there are multiple messagesets 
sub _messageset_decode_ng {
    my ($buffer, $length) = @_;
    my $d = {
        position => \$buffer->{position},
        data => \$buffer->{data},
    };
    my $end = $length + $buffer->{position};
    return _messageset_decode($d, $end);
}

##
# Expects a buffer_hr and an end position
#
# Returns an array of messages
##
# TODO rework
sub _messageset_decode {
    my $buffer_hr   = shift;
    my $end         = shift;

    my $pos = $buffer_hr->{position};
    my $response = $buffer_hr->{data};

    if ( DEBUG ) {
        print STDERR "Decoding message set:\n"
            ."pos           = '$$pos'\n"
            ."end           = '$end'\n"
            #."response      = '".unpack("H*", $$response)."'\n"
            ;
    }

    my $decoded_messages = [];

    # 12 = length( OFFSET + LENGTH )
    while ( $end - $$pos > 12 ) {
        my $offset = BITS64     # OFFSET
          ? unpack("x$$pos q>", $$response )
          : Kafka::Int64::unpackq( unpack( "x$$pos a8", $$response ) );
        $$pos += 8;
        my $length = unpack("x$$pos l>", $$response);
        $$pos += 4;

        if (DEBUG) {
            print STDERR "Decoding message at:\n"
                ."offset:   = $offset\n"
                ."length:   = $length\n"
                ;
        }

        last if ( ( $end - $$pos ) < $length ); # Incomplete message

        my $msg_array = _decode_message( $buffer_hr, $length ); #$length, $response);
        for my $message (@$msg_array) {
            $message->{valid} = ! $message->{error};
            $message->{offset} = $offset;
            push @$decoded_messages, $message;
        }

    }

    if ( DEBUG )
    {
        print STDERR "Messages:\n";
        for ( my $idx = 0; $idx <= $#{$decoded_messages} ; $idx++ )
        {
            my $message = $decoded_messages->[ $idx ];
            print STDERR
                 "index              = $idx\n"
                ."LENGTH             = $message->{length}\n"
                ."MAGIC              = $message->{magic}\n"
                .( $message->{magic} ? "COMPRESSION        = $message->{compression}\n" : "" )
                ."CHECKSUM           = $message->{checksum}\n"
                ."PAYLOAD            = $message->{payload}\n"
                ."valid              = $message->{valid}\n"
                ."error              = $message->{error}\n";
        }
    }

    return $decoded_messages;
}

##
# Expects a buffer_hr and the length of the message to expect.
#
# The length is used to calculate the checksum of the message.
#
# Returns an array of messages.
##
sub _decode_message {
    my $buffer_hr = shift; # The buffer_hr
    my $length = shift; # The length of the message

    my $response = $buffer_hr->{data};
    my $pos = $buffer_hr->{position};

    my $message = {};
    $message->{error} = "";
    $message->{length} = $length;

    my ($key_size, $payload_size);

    (
        $message->{checksum},
        $message->{magic},
        $message->{attributes},
        $key_size,
    ) = unpack("x$$pos
        L>          # Checksum
        c           # MagicByte
        c           # Attributes
        l>          # Key size
        ", $$response);
    $$pos += 4; # Just the checksum
    my $checksum_calculated = crc32(
        unpack("x$$pos a".($length-4), $$response )
    );
    $$pos += 1 + 1 + 4; # Consume the bytes and key size

    if ($message->{checksum} != $checksum_calculated) {
        $message->{error} = $Kafka::ERROR[ERROR_CHECKSUM_ERROR];
        return \[$message]; # TODO TEST
    }

    if ($key_size >= 0) {
        $message->{key} = unpack("x$$pos a$key_size", $$response);
        $$pos += $key_size;
    }

    $payload_size = unpack("x$$pos l>", $$response);
    $$pos += 4;
    if ($payload_size >= 0) {
        $message->{payload} = unpack("x$$pos a$payload_size", $$response);
        $$pos += $payload_size;
    }

    if ($message->{magic}) {
        print STDERR "Magic is set: $message->{magic}\n"; # TODO fix
        die("[BUG] Handling more magic is not implemented.");
    }

    my $out;
    # Parse the attributes. This should be based on the magic version
    $message->{compression} = ($message->{attributes} & 0x3);
    if ($message->{compression} == COMPRESSION_NO_COMPRESSION) {
        $out = [$message]; # Easy
    } else {
        # Handle compression
        my $pos = 0;
        my $data = '';
        if ($message->{compression} == COMPRESSION_GZIP) {
            gunzip(\$message->{payload} => \$data);
        } elsif ($message->{compression} == COMPRESSION_SNAPPY) {
            die("[BUG] SNAPPY compressed messaged has not been implemented.");
        } else {
            die("[ERROR] Unknown compression type found.");
        }
        my $buf_hr = {
            data => \$data,
            position => \$pos,
        };
        $out = _messageset_decode( $buf_hr, bytes::length($data) );
        $buf_hr = undef;
    }

    return $out;
}

# FETCH Request ----------------------------------------------------------------

##
# Used to pack a fetch request
#
# Expects:
#   * a hash of the data to pack: {
#       topic01 => {
#           part01 => [offset, max_bytes],
#           part02 => [offset, max_bytes],
#           },
#       topic02 => { ... },
#    }
#   * (Optional) a hash of options.
#       - max_wait_time     XXX not implemented (max wait time)
#       - min_bytes         XXX not implemented
# Returns:
#   * a reference to the packed data
##
# TODO impl, test, doc
sub fetch_request_ng {
    my ($data, $opts) = @_;

    # Encode the header for fetch request
    my $packed = pack("
        l>      # ReplicaId
        l>      # Max Wait time
        l>      # Min Bytes
        l>      # Number of topics
        ", REPLICAID, MAX_WAIT_TIME, MIN_BYTES, scalar(keys $data));

    while (my ($topic, $tdata) = each($data)) {
        $packed .= pack("
            s>/a    # Topic 
            l>      # Number of partitions
            ", $topic, scalar(keys($tdata)));

        while (my ($partition, $pdata) = each($tdata)) {
            my ($offset, $max_bytes) = @$pdata;
            $packed .= pack("l>", $partition)
                    .pack64($offset)
                    .pack("l>", $max_bytes);
            if ( DEBUG )
            {
                print STDERR "Fetch request:\n"
                    ."PARTITION          = $partition\n"
                    ."OFFSET             = $offset\n"
                    ."MAX_BYTES          = $max_bytes\n";
            }
        }
    }
    return \$packed;
}


sub fetch_request {
    my $topic           = _STRING( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    my $partition       = shift;
    my $offset          = shift;
    my $max_size        = _POSINT( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    return _error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );
    ( ref( $offset ) eq "Math::BigInt" and $offset >= 0 ) or defined( _NONNEGINT( $offset ) ) or return _error( ERROR_MISMATCH_ARGUMENT );

    # Encode the header for fetch request
    my $encoded = pack("
        l>      # ReplicaId
        l>      # Max Wait time
        l>      # Min Bytes
        ", REPLICAID, MAX_WAIT_TIME, MIN_BYTES);

    # Encode the array of topics
    $encoded .= pack("
        l>      # Number of array elements
        s>/a   # Topic Name size and string
        ", 1, $topic);

    # Encode the array of partition requests for this topic
    $encoded .= pack("
            l>      # Number of array elements
            l>      # Partition
            ", 1, $partition)
        .( BITS64 ? pack( "q>", $offset + 0 ) : Kafka::Int64::packq( $offset + 0 ) )   # OFFSET
        .pack("l>   # Max bytes
            ", $max_size);


    if ( DEBUG )
    {
        print STDERR "Fetch request:\n"
            ."PARTITION          = $partition\n"
            ."OFFSET             = $offset\n"
            ."MAX_SIZE           = $max_size\n";
    }

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            APIKEY_FETCH,
            CLIENT_ID,
            ).$encoded;

    return $encoded;
}

# OFFSETS Request --------------------------------------------------------------

##
# used to request offsets for a set of topics and partitions
#
# Expects:
#   * a hash
#   {
#   topic01 => [ [partitionId, time, max_number] ],
#   topic02 => [ [0, -1, 1], [1, -1, 1], [2, -1, 1] ],
#   }
# Returns:
#   * a reference to the packed data
##
# TODO: impl, test
sub offsets_request_ng {
    my $input = _HASHLIKE( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    my $packed = pack("
        l>  # ReplicaID, always -1
        l>  # Number of topics
        ", -1, scalar(keys $input)
    ); 

    while (my ($topic, $tdata) = each($input)) {
        # TODO check arguments. topic = string, $tdata = array
        $packed .= pack("
            s>/a    # Topic
            l>      # Number of partitions
            ", $topic, scalar(@$tdata)
        );
        foreach my $partition_data (@$tdata) {
            # TODO _ARRAY( $partition_data ) or error out
            my ($partitionId, $time, $max_number) = @$partition_data;
            # TODO check arguments
            $packed .= pack("l>", $partitionId); # Partition ID
            $packed .= ( BITS64 
                        ? pack( "q>", $time + 0 ) 
                        : Kafka::Int64::packq( $time + 0 ) );   # TIME
            $packed .= pack("N", $max_number);
            if ( DEBUG )
            {
                print STDERR "Offsets request:\n"
                ."PARTITION          = $partitionId\n"
                ."TIME               = $time\n"
                ."MAX_NUMBER         = $max_number\n"
                ;
            }
        }
    }

    return \$packed;
}

sub offsets_request {
    my $topic           = _STRING( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    my $partition       = shift;
    my $time            = shift;
    my $max_number      = _POSINT( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    return _error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );
    ( ref( $time ) eq "Math::BigInt" ) or defined( _NUMBER( $time ) ) or return _error( ERROR_MISMATCH_ARGUMENT );
    $time = int( $time );
    return _error( ERROR_MISMATCH_ARGUMENT ) if $time < -2;

    # TODO Allow multiple partition requests?
    my $encoded = pack("l>l>", -1, 1); # Replica id and topic count
    $encoded .= pack("s>/a", $topic);
    $encoded .= pack("l>l>", 1, $partition);
    $encoded .= ( BITS64 ? pack( "q>", $time + 0 ) : Kafka::Int64::packq( $time + 0 ) );   # TIME
    $encoded .= pack( "
                      N                                   # MAX_NUMBER
                      ",
                      $max_number,
            );

    if ( DEBUG )
    {
        print STDERR "Offsets request:\n"
            ."TIME               = $time\n"
            ."MAX_NUMBER         = $max_number\n"
            ."ENCODED_LEN        = ".bytes::length( $encoded )."\n"
        ;
    }

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            APIKEY_OFFSETS,
            CLIENT_ID, # ClientID
            ).$encoded;

    return $encoded;
}

################################################################################
# Responses Wire Format

# Response Header

sub _response_header_decode {
    my $response = shift;                       # requires a reference

    my $header = {};

    # 6 = length( RESPONSE_LENGTH + ERROR_CODE )
    if ( bytes::length( $$response ) >= 6 )
    {
# will unpack exception if the message structure disrupted
        $position = 0;
        (
            $header->{response_length},
            $header->{correlation_id},
        ) = unpack( "
            l>                                       # RESPONSE_LENGTH
            l>                                       # CorrelationID
            ", $$response );
        $position += 8;

        if ( DEBUG )
        {
            print STDERR "Response Header:\n"
                ."RESPONSE_LENGTH    = $header->{response_length}\n"
                ."CORRELATION_ID     = $header->{correlation_id}\n";
        }
    }

    return $header;
}

# Expects the response data
# Returns (partitionid, partitiondata)
sub _partitionmetadata_decode {
    my $response = shift;

    my $data = {};

    (
        $data->{error_code},
        $data->{partid},
        $data->{leader},
    ) = unpack("x$position
        s>          # Partition Error Code
        l>          # Partition ID
        l>          # Leader NodeID
        ", $$response);
    $position += 10;

    my @replicas = unpack("x$position
        l>/(l>)     # Replicas
        ", $$response);
    $position += 4 + 4*scalar(@replicas);

    my @isr = unpack("x$position
        l>/(l>)     # Insync Replicas
        ", $$response);
    $position += 4 + 4*scalar(@isr);

    if (DEBUG) {
        print STDERR ""
        ."PARTITION             = $data->{partid}\n"
        ."ERROR                 = $data->{error_code}\n"
        ."LEADER                = $data->{leader}\n"
        ."COUNT REPLICAS        = ".scalar(@replicas)."\n"
        ."REPLICAS              = ".join(",", @replicas)."\n"
        ."COUNT ISR             = ".scalar(@isr)."\n"
        ."ISR                   = ".join(",", @isr)."\n"
        ;
    }
    $data->{replicas} = \@replicas;
    $data->{isr} = \@isr;
    return ($data->{partid}, $data);
}


##
# Used to decode the metadata for each partition. This should only
# be used in the metadata response decoder. I feel it is long enough
# to pull out.
##
sub _partitionmetadata_decode_ng {
    my $response = shift;

    my $data = {};

    (
        $data->{error_code},
        $data->{partid},
        $data->{leader},
    ) = unpackRes("
        s>          # Partition Error Code
        l>          # Partition ID
        l>          # Leader NodeID
        ", $response);
    $response->{position} += 10;

    my @replicas = unpackRes("l>/(l>)", $response);
    $response->{position} += 4 + 4*scalar(@replicas);

    my @isr = unpackRes("l>/(l>)", $response);
    $response->{position} += 4 + 4*scalar(@isr);

    if (DEBUG) {
        print STDERR ""
        ."PARTITION             = $data->{partid}\n"
        ."ERROR                 = $data->{error_code}\n"
        ."LEADER                = $data->{leader}\n"
        ."COUNT REPLICAS        = ".scalar(@replicas)."\n"
        ."REPLICAS              = ".join(",", @replicas)."\n"
        ."COUNT ISR             = ".scalar(@isr)."\n"
        ."ISR                   = ".join(",", @isr)."\n"
        ;
    }
    $data->{replicas} = \@replicas;
    $data->{isr} = \@isr;
    return ($data->{partid}, $data);
}

# METADATA Response

##
# Used to decode a metadata response.
##
sub metadata_response_ng {
    my $response = shift;

    my $decoded = {};

    # Unpack the broker metadata
    my $num_brokers = unpackRes("l>", $response);
    $response->{position} += 4;

    for my $i (1..$num_brokers) {
        my ($nodeid, $strlen, $host, $port) = unpackRes("
            l>      # NodeId
            s>X2    # Strlen and go back
            s>/a    # Host
            l>      # Port
            ", $response);
        $response->{position} += 10 + $strlen;
        $decoded->{brokers}{$nodeid} = [$host, $port];

        if ( DEBUG ) {
            print STDERR "Decoded broker($i) information:\n"
                    ."NODEID        = $nodeid\n"
                    ."HOST          = $host\n"
                    ."PORT          = $port\n"
                    ;
        }
    }

    # Unpack the Topic Metadata
    my $num_topics = unpackRes("l>", $response);
    $response->{position} += 4;
    for my $i (1..$num_topics) {
        my ($error_code, $strlen, $topic, $num_partitions) = unpackRes("
            s>      # Topic Error Code
            s>X2    # Strlen
            s>/a    # Topic
            l>      # Number of partitions for this topic
        ", $response);
        $response->{position} += 8 + $strlen;

        if (DEBUG) {
            print STDERR "Decoded topic($i) information:\n"
                ."NAME              = $topic\n"
                ."PARTITION COUNT   = $num_partitions\n"
                ;
        }

        $decoded->{topics}{$topic} = {
            error_code => $error_code,
            num_partitions => $num_partitions
        };

        # Decode each partition
        for my $j (1..$num_partitions) {
            my ($partid, $data) = _partitionmetadata_decode_ng($response);
            $decoded->{topics}{$topic}{partitions}{$partid} = $data;

            if (DEBUG) {
                print STDERR "Partition($j) data:\t$data\n";
            }
        }
    }
    return $decoded;
}

sub metadata_response {
    my $response = _SCALAR( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    _STRING( $$response ) or return _error( ERROR_MISMATCH_ARGUMENT );

    # 16 minimum size of metadata response
    return _error( ERROR_MISMATCH_ARGUMENT ) if bytes::length( $$response ) < 16;

    my $decoded = {};
    $decoded->{header} = _response_header_decode( $response );

    # Unpack the broker metadata
    my $num_brokers = unpack("x$position l>", $$response);
    $position += 4;

    for my $i (1..$num_brokers) {
        my ($nodeid, $strlen, $host, $port) = unpack("x$position
            l>      # NodeId
            s>X2    # Strlen and go back
            s>/a    # Host
            l>      # Port
            ", $$response);
        $position += 10 + $strlen;

        if ( DEBUG ) {
            print STDERR "Decoded broker information:\n"
                    ."NODEID        = $nodeid\n"
                    ."HOST          = $host\n"
                    ."PORT          = $port\n"
                    ;
        }

        $decoded->{brokers}{$nodeid} = [$host, $port];
    }

    # Unpack the Topic Metadata
    my $num_topics = unpack("x$position l>", $$response);
    $position += 4;

    for my $i (1..$num_topics) {
        my ($error_code, $strlen, $topic, $num_partitions) = unpack("x$position
            s>      # Topic Error Code
            s>X2    # Strlen
            s>/a    # Topic
            l>      # Number of partitions for this topic
        ", $$response);
        $position += 8 + $strlen;

        if (DEBUG) {
            print STDERR "Decoded Topic:\n"
                ."NAME              = $topic\n"
                ."PARTITION COUNT   = $num_partitions\n"
                ;
        }

        $decoded->{topics}{$topic} = {
            error_code => $error_code,
            num_partitions => $num_partitions
        };

        # Decode each partition
        for my $j (1..$num_partitions) {
            my ($partid, $data) = _partitionmetadata_decode($response);
            $decoded->{topics}{$topic}{partitions}{$partid} = $data;
        }
    }
    return $decoded;
}

# PRODUCE Response

##
# Used to decode the response to a produce request.
##
sub produce_response_ng {
    my $response = shift;
    my $ret = {};

    my $num_topics = unpackRes("l>", $response);
    $response->{position} += 4;
    foreach my $i (1..$num_topics) {
        my ($strlen, $topic, $num_partitions) 
            = unpackRes("s>X2 s>/a l>", $response);
        $response->{position} += 2 + 4 + $strlen;
        $ret->{$topic} = {};

        foreach my $j (1..$num_partitions) {
            my ($partition, $error_code) = unpackRes("l> s>", $response);
            $response->{position} += 4 + 2;
            my $offset = unpackRes64($response); # 64 bit offset
            $response->{position} += 8;

            $ret->{$topic}->{$partition} = {
                error_code => $error_code,
                offset => $offset,
            };
        }
    }
    return $ret;
}

sub produce_response {
    die("[BUG] Not implemented silly one.");
    #return $decoded;
}

#   None

# FETCH Response

##
# Used to decode a fetch response.
##
sub fetch_response_ng {
    my $response = shift;
    my $ret = {};

    my $num_topics = unpackRes("l>", $response);
    $response->{position} += 4;

    foreach my $i (1..$num_topics) {
        my ($strlen, $topic, $num_partitions) =
            unpackRes("s>X2 s>/a l>", $response);
        $response->{position} += 2 + 4 + $strlen;

        $ret->{$topic} = {};
        foreach my $j (1..$num_partitions) {
            my ($partition, $error_code, $highwaterp, $messageset_size) =
                unpackRes("l> s> a8 l>", $response);
            $response->{position} += 4 + 2 + 8 + 4; # 18
            # highwaterp is a packed int64
            if (DEBUG) {
                print STDERR ""
                    ."partition         = $partition\n"
                    ."error code        = $error_code\n"
                    ."message set size  = $messageset_size\n"
                    ;
            }
            # Get a reference to the array of messages from this data
            my $messages = _messageset_decode_ng($response, $messageset_size);
            $ret->{$topic}->{$partition} = {
                error_code => $error_code,
                messages => $messages,
            };
        }
    }
    return $ret;
}

sub fetch_response {
    my $response = _SCALAR( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    _STRING( $$response ) or return _error( ERROR_MISMATCH_ARGUMENT );
    # 6 = length( RESPONSE_LENGTH + ERROR_CODE )
    return _error( ERROR_MISMATCH_ARGUMENT ) if bytes::length( $$response ) < 6;

    my $decoded = {};
    if ( scalar keys %{ $decoded->{header} = _response_header_decode( $response ) } )
    {
        my $topic_count = unpack("x$position l>", $$response);
        $position += 4;
        for my $i (1 .. $topic_count) {
            my ($strlen, $topic, $partition_count) =
                unpack("x$position s>X2 s>/a l>", $$response);
            $position += 6 + $strlen;

            for my $j (1 .. $partition_count) {
                my ($partition, $error_code, $highwaterp, $messageset_size) =
                    unpack("x$position l> s> a8 l>", $$response);
                $position += 18;
                my $highwater = BITS64     # OFFSET
                  ? unpack("q>", $highwaterp )
                  : Kafka::Int64::unpackq($highwaterp);

                if (DEBUG) {
                    print STDERR ""
                        ."partition         = $partition\n"
                        ."error code        = $error_code\n"
                        ."message set size  = $messageset_size\n"
                        ;
                }
                $decoded->{header}->{error_code} = $error_code;
                my $buf_obj = { data => $response, position => \$position };
                $decoded->{messages} = _messageset_decode( $buf_obj, $position+$messageset_size ) unless $error_code;
            }
        }
    }

    return $decoded;
}

# OFFSETS Response

##
# Used to decode offsets responses.
##
sub offsets_response_ng {
    my $response = shift;
    # TODO check for a hash
    my $ret = {};

    my $num_topics = unpackRes("l>", $response);
    $response->{position} += 4;
    foreach my $i (1..$num_topics) {
        my ($strlen, $topic, $num_partitions) 
            = unpackRes("s>X2 s>/a l>", $response);
        $response->{position} += 2 + 4 + $strlen;

        $ret->{$topic} = {};
        foreach my $j (1..$num_partitions) {
            my ($partition, $error_code, $num_offsets) 
                = unpackRes("l> s> l>", $response);
            $response->{position} += 4 + 2 + 4;

            my $offsets = [];
            foreach my $k (1..$num_offsets) {
                my $offset = unpackRes64($response); # 64 bit offset
                $response->{position} += 8;
                push(@{$offsets}, $offset);
            }
            $ret->{$topic}->{$partition}->{offsets} = $offsets;
            $ret->{$topic}->{$partition}->{error_code} = $error_code;

            if (DEBUG) {
                warn("[BUG] Error code for offsets request is not checked");
                print STDERR "Offsets response topic: $topic, part: $partition\n"
                    ."offsets:  = ".join(",",$offsets)."\n"
                    ."error:    = $error_code\n"
                    ;
            }
        }
    }
    return $ret;
}

sub offsets_response {
    my $response = _SCALAR( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    _STRING( $$response ) or return _error( ERROR_MISMATCH_ARGUMENT );
    # 6 = length( RESPONSE_LENGTH + ERROR_CODE )
    return _error( ERROR_MISMATCH_ARGUMENT ) if bytes::length( $$response ) < 6;

    my $decoded = {};
    $decoded->{header} = _response_header_decode( $response );

    unless ( $decoded->{header}->{error_code} )
    {
        $decoded->{number_topics} = unpack("x$position l>", $$response);
        $position += 4;

        if ($decoded->{number_topics} > 1) {
            die("[BUG] More than one topic offset response received (Not implemented).");
        }

        $decoded->{topics} = ();
        my $i = 0;
        for ($i = 0; $i < $decoded->{number_topics}; $i++) {
            my ($strlen, $topic, $partition_count)
                = unpack("x$position s>X2 s>/a l>", $$response);
            $position += 6 + $strlen;

            if ($partition_count > 1) {
                die("[BUG] Not implemented.");
            }

            my $j = 0;
            for ($j = 0; $j < $partition_count; $j++) {
                my ($partition, $error_code, $offset_count)
                    = unpack("x".$position."l> s> l>", $$response);
                $position += 10;

                $decoded->{error} = $error_code;
                $decoded->{number_offsets} = $offset_count;

                my $offsets = [];
                my $o = 0;
                for ($o = 0; $o < $offset_count; $o++) {
                    my $offset = BITS64     # OFFSET
                      ? unpack("x$position q>", $$response )
                      : Kafka::Int64::unpackq( unpack( "x$position a8", $$response ) );
                    $position += 8;
                    push @{$offsets}, $offset;
                }
                $decoded->{offsets} = $offsets;
            }
        }
        $decoded->{error} = ( $decoded->{number_offsets} == scalar( @{$decoded->{offsets}} ) ) ? "" : $Kafka::ERROR[ERROR_NUMBER_OF_OFFSETS];

        if ( DEBUG )
        {
            print STDERR "Offsets response:\n"
                ."NUMBER_OFFSETS     = $decoded->{number_offsets}\n"
                ."error              = $decoded->{error}\n";
            for ( my $idx = 0; $idx <= $#{$decoded->{offsets}} ; $idx++ )
            {
                print STDERR "OFFSET             = $decoded->{offsets}->[ $idx ]\n";
            }
        }
    }

    return $decoded;
}

################################################################################

1;

__END__

=head1 NAME

Kafka::Protocol - functions to process messages in the Apache Kafka's 0.8 Wire
Format

=head1 VERSION

This documentation refers to C<Kafka::Protocol> version 0.22

=head1 SYNOPSIS

=head1 DESCRIPTION

When producing messages, the driver has to specify what topic and partition
to send the message to. When requesting messages, the driver has to specify
what topic, partition, and offset it wants them pulled from.

While you can request "old" messages if you know their topic, partition, and
offset, Kafka does not have a message index. You cannot efficiently query Kafka
for all messages written between 30 and 35 minutes ago.

The main features of the C<Kafka::Protocol> module are:

=over 3

=item *

Supports parsing the Apache Kafka Wire Format protocol.

=item *

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules

L<Kafka::IO|Kafka::IO> - object interface to socket communications with
the Apache Kafka server

L<Kafka::Producer|Kafka::Producer> - object interface to the producer client

L<Kafka::Consumer|Kafka::Consumer> - object interface to the consumer client

L<Kafka::Message|Kafka::Message> - object interface to the Kafka message
properties

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's wire format

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems 

L<Kafka::Mock|Kafka::Mock> - object interface to the TCP mock server for testing

A wealth of detail about the Apache Kafka and Wire Format:

Main page at L<http://incubator.apache.org/kafka/>

Wire Format at L<http://cwiki.apache.org/confluence/display/KAFKA/Wire+Format/>

Writing a Driver for Kafka at
L<http://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka>

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2013 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
