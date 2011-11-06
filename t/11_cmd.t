use Test::More;
use Data::Dumper; 

use Test::HadoopSingleNode;

my $length = 1024;

my @tests = (
    { command => [ 'perl', '-e', q{print 'a'} ], out => 'a', err => '', code => 0 },
    { command => [ 'perl', '-e', q{exit 0} ], out => '', err => '', code => 0 },
    { command => [ 'perl', '-e', q{exit 1} ], out => '', err => '', code => 1 },
    { command => [ 'perl', '-e', q{exit 255} ], out => '', err => '', code => 255 },
    { command => [ 'perl', '-e', qq{print 'a' x ($length ** 2)} ], out => 'a' x ($length ** 2), err => '', code => 0 },
    { command => [ 'perl', '-e', qq{print STDERR 'a' x ($length ** 2)} ], out => '', err => 'a' x ($length ** 2), code => 0 },
    { command => [ 'perl', '-e', qq{print 'a' x ($length ** 2); print STDERR 'b' x ($length ** 2)} ], out => 'a' x ($length ** 2), err => 'b' x ($length ** 2), code => 0 },
);

my $hadoop = Test::HadoopSingleNode->new()
    or plan skip_all => 'not found hadoop bin';

for my $test (@tests) {
    my $retcode = $hadoop->cmd(@{$test->{command}});
    is $retcode, $test->{code}, "exit code";
}

for my $test (@tests) {
    my ($retcode, $out, $err) = $hadoop->cmd(@{$test->{command}});
    is $retcode, $test->{code}, "exit code";
    is $out,     $test->{out},  "output";
    is $err,     $test->{err},  "stderr";
}

done_testing;
