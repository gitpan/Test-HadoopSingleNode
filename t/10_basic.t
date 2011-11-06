use strict;
use warnings;
use Carp;
use Data::Dumper;
use Path::Class;
use File::Which qw(which);

use Test::More;
use Test::HadoopSingleNode;

my $hadoop = Test::HadoopSingleNode->new(
    default_hadoop_conf_dir => file(__FILE__)->parent->subdir('default_conf'),
) or plan skip_all => 'not found hadoop bin';

diag Dumper $hadoop;

$hadoop->start_all();

$hadoop->construct_fixture(
    '/data/dist/test' => file(__FILE__)->parent->file('local.tsv')
);

{
    my $ret = $hadoop->cmd(
        scalar(which('hadoop')), $hadoop->_add_hadoop_opt,
        'fs', '-test', '-e', "/data/dist/test");
    is($ret, 0, "File found in hdfs");
}

{
    my $ret = $hadoop->cmd(
        scalar(which('hadoop')), $hadoop->_add_hadoop_opt,
        'fs', '-test', '-e', "/data/not/found");
    is($ret, 1, "Not found in hdfs");
}

$hadoop->stop_all();

done_testing;
