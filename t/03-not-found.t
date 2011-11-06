use strict;
use warnings;

use Test::HadoopSingleNode;

use Test::More tests => 1;

$ENV{PATH} = '/nonexistent';

ok(! defined Test::HadoopSingleNode->new());
