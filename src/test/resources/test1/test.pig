REGISTER build/libs/piggy-converter-0.1.jar;

-- PiggyConverterUDF(scriptName, outputField1, outputField2, ...)
--   scriptName  : Piggy Converter script name followed by '#script.pc' (script will be added to distributed cache).
--   outputField : You can specify all input columns by "*".
-- *** All column used in Piggy Converter script must appear in input or output schema.
DEFINE Convert org.rhase.piggyconverter.PiggyConverterUDF('$script#script.pc',  '*', 'numid:int', 'memo:chararray'); 

a = LOAD '$input' AS (access_time:datetime, userid:chararray, url:chararray);

b = FOREACH a GENERATE Convert(*) as conved;
c = FILTER b BY conved IS NOT null;
d = FOREACH c GENERATE flatten(conved);
STORE d INTO '$output';
