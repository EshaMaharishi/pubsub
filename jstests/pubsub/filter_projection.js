// subscribe without filter or projection
var subRegular = db.runCommand({ subscribe: "A" })['subscriptionId'];

// subscribe with filter and projection
var subBoth = db.runCommand({ subscribe: "A",
                              filter: { count: { $gt: 3 } },
                              projection: { count: 1 }
                            })['subscriptionId'];

// publish
for(var i=0; i<6; i++){
    db.runCommand({ publish: "A", message: { body : "hello", count : i } });
    // ensure that this test, which depends on in-order delivery,
    // runs on systems with only millisecond granularity
    sleep(1);
}

// poll on all subscriptions as array
var arr = [subRegular, subBoth];
var res = db.runCommand({ poll : arr });

// verify correct messages received
var resRegular = res["messages"][subRegular.str]["A"];
for(var i=0; i<6; i++)
    assert.eq(resRegular[i]["count"], i);

var resBoth = res["messages"][subBoth.str]["A"];
for(var i=0; i<2; i++){
    assert.eq(resBoth[i]["count"], i+4);
    assert.eq(resBoth[i]["body"], undefined);
}

