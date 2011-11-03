use inc::Module::Install;
name 'Test-HadoopSingleNode';
all_from 'lib/Test/HadoopSingleNode.pm';
readme_from 'lib/Test/HadoopSingleNode.pm';
readme_markdown_from 'lib/Test/HadoopSingleNode.pm';
readme_pod_from 'lib/Test/HadoopSingleNode.pm';
githubmeta;

requires 'Carp';
requires 'File::Temp';
requires 'Test::TCP';
requires 'File::Which';
requires 'Proc::Guard';
requires 'IPC::Open3';
requires 'IPC::Open3::Simple';
requires 'IO::Select';
requires 'IO::Handle';
requires 'Symbol';
requires 'IPC::Run';
requires 'XML::Simple';
requires 'Data::Rmap';
requires 'Data::Dumper';
requires 'Path::Class';
requires 'File::Copy';

test_requires 'Test::More';
test_requires 'Path::Class';

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
