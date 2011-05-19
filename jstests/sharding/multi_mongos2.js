// multi_mongos2.js
// This tests sharding an existing collection that both shards are aware of (SERVER-2828)


// setup sharding with two mongos, s1 and s2
s1 = new ShardingTest( "multi_mongos1" , 2 , 1 , 2 );
s2 = s1._mongos[1];

s1.adminCommand( { enablesharding : "test" } );
s1.adminCommand( { shardcollection : "test.foo" , key : { num : 1 } } );

s1.config.databases.find().forEach( printjson )

s1.getDB('test').existing.insert({_id:1})
assert.eq(1, s1.getDB('test').existing.count({_id:1}));
assert.eq(1, s2.getDB('test').existing.count({_id:1}));

s2.adminCommand( { shardcollection : "test.existing" , key : { _id : 1 } } );
s2.adminCommand( { split : "test.existing" , find : { _id : 5 } } )

//assert.eq(true, s1.getDB('test').existing.stats().sharded); //SERVER-2828
assert.eq(true, s2.getDB('test').existing.stats().sharded);


if (0) // This should work but doesn't due to SERVER-2828
    res = s1.getDB( "admin" ).runCommand( { moveChunk: "test.existing" , find : { _id : 1 } , to : s1.getOther( s1.getServer( "test" ) ).name } );
else
    res = s2.getDB( "admin" ).runCommand( { moveChunk: "test.existing" , find : { _id : 1 } , to : s1.getOther( s1.getServer( "test" ) ).name } );

assert.eq(1 , res.ok, tojson(res));

printjson( s2.adminCommand( {"getShardVersion" : "test.existing" } ) )
printjson( new Mongo(s1.getServer( "test" ).name).getDB( "admin" ).adminCommand( {"getShardVersion" : "test.existing" } ) )


assert.eq(1, s1.getDB('test').existing.count({_id:1})); // SERVER-2828
assert.eq(1, s2.getDB('test').existing.count({_id:1}));

s1.stop();
