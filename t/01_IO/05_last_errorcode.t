#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 5;

use Kafka::IO;

use Kafka::Mock;

my $server;

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
isa_ok( $server, 'Kafka::Mock');

my $port = $server->port;
ok $port, "server port = $port";

my $io;

$io = Kafka::IO->new(
    host        => "localhost",
    port        => $port,
    timeout     => 0.5,
    RaiseError  => 0
    );
isa_ok( $io, 'Kafka::IO');

ok( !defined( $io->last_errorcode ), "not defined last_errorcode" );
$io->send( [] );
ok( defined( $io->last_errorcode ), "last_error = ".$io->last_errorcode );

$io->close;

$server->close;
