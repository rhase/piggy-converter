REGISTER build/libs/piggy-converter-0.1.jar;

-- piggy converter script distcache name must be 'script.pc'.
DEFINE CONVERT org.rhase.piggyconverter.PiggyConverterUDF(
  '/test/test.pc#script.pc',  '*', 'id_a:chararray', 'id_b:chararray'); 

a = load '/test/access.log' as (access_time:datetime, userid:chararray, url:chararray);

-- You can specify all input columns by "*".
b = foreach a generate CONVERT(*) as conved;

c = foreach b generate flatten(conved);
store c into '/test/output';