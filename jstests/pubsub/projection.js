// subscribe without projection 
var subRegular = db.runCommand({ subscribe: "A" })['subscriptionId'];

// subscribe with projection
var subProjection = db.runCommand({ subscribe: "A", projection: { count: 1 } })['subscriptionId'];

// publish                                                                                          
for(var i=0; i<6; i++){                                                                             
    db.runCommand({ publish: "A", message: { body : "hello", count : i } });                        
    // ensure that this test, which depends on in-order delivery,                                   
    // runs on systems with only millisecond granularity                                            
    sleep(1);                                                                                       
}

// poll on subscriptions as array                                                               
var arr = [subRegular, subProjection];                                              
var res = db.runCommand({ poll : arr });                                                           

// verify correct messages received
var resRegular = res["messages"][subRegular.str]["A"];                                                      
for(var i=0; i<6; i++)                                                                              
    assert.eq(resRegular[i]["count"], i);

var resProjection = res["messages"][subProjection.str]["A"];                                        
for(var i=0; i<6; i++){                                                                             
    assert.eq(resProjection[i]["count"], i);                                                        
    assert.eq(resProjection[i]["body"], undefined);                                                 
} 
