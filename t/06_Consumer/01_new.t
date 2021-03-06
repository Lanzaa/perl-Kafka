#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Consumer::new

use lib 'lib';

use Test::More skip_all => "because tests not updated for Kafka 0.8 protocol";
#use Test::More tests => 34;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

use Kafka::Mock;
use Kafka::IO;

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my ( $server, $io, $consumer, $err );

sub my_io {
    my $io      = shift;

    $$io = Kafka::IO->new(
        host        => "localhost",
        port        => $server->port,
        );
}

sub my_close {
    $consumer->close if $consumer;
#    $consumer  = $io = undef;
    $consumer = undef;
}

# -- verification of the IO objects creation

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    );
isa_ok( $server, 'Kafka::Mock');

my_io( \$io );
isa_ok( $io, 'Kafka::IO');
my_close();

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module

BEGIN { use_ok 'Kafka::Consumer' }

# -- verify response to arguments

my_io( \$io );

$consumer = Kafka::Consumer->new();
$err = $@; chomp $err;
isnt( defined( $consumer ), 1, "threw Exception because without args" );

$consumer = Kafka::Consumer->new( anything => "any" );
$err = $@; chomp $err;
isnt( defined( $consumer ), 1, "threw Exception because only unknown arg" );

my_close();

# IO - incorrect
my_io( \$io );
foreach my $io ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $server ) )
{
    dies_ok { Kafka::Consumer->new(
        IO => $io,
        RaiseError  => 1
        ) } "expecting to die (io = ".( $io || "" ).")";
}
my_close();

# IO - correct
my_io( \$io );
lives_ok { $consumer = Kafka::Consumer->new(
    IO          => $io,
    RaiseError  => 1
    ); } 'expecting to live';
isa_ok( $consumer, 'Kafka::Consumer');
my_close();

# RaiseError - incorrect
my_io( \$io );
foreach my $RaiseError ( ( undef, "", "nothing", -1 ) )
{
    $consumer = Kafka::Consumer->new(
        IO          => $io,
        RaiseError  => $RaiseError
        );
    my $err = $@;
    chomp $err;
    isnt( defined( $consumer ), 1, "threw Exception: $err (RaiseError = ".( $RaiseError || "" ).")" );
}
my_close();

# RaiseError - correct
foreach my $RaiseError ( ( 1, "1", 0, 10 ) )
{
    my_io( \$io );
    $consumer = Kafka::Consumer->new(
        IO          => $io,
        RaiseError  => $RaiseError
        );
    my $err = $@;
    isa_ok( $consumer, 'Kafka::Consumer');
    pass "the normal args";
    my_close();
}

# RaiseError - default
my_io( \$io );
$consumer = Kafka::Consumer->new(
    IO          => $io,
    );
isa_ok( $consumer, 'Kafka::Consumer');

# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
my_close();
$server->close;
