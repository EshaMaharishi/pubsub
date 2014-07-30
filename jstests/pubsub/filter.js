// subscribe without filter                                                                         
var subRegular = db.runCommand({ subscribe: "A" })['subscriptionId'];                                   
                                                                                                    
// subscribe with filter                                                                            
var subFilter = db.runCommand({ subscribe: "A", filter: { count: { $gt: 3 } } })['subscriptionId'];

// publish                                                                                          
for(var i=0; i<6; i++){                                                                             
    db.runCommand({ publish: "A", message: { body : "hello", count : i } });                        
    // ensure that this test, which depends on in-order delivery,                                   
    // runs on systems with only millisecond granularity                                            
    sleep(1);                                                                                       
}

// poll on all subscriptions as array                                                               
var arr = [subRegular, subFilter];                                              
var res = db.runCommand({ poll : arr });                                                            
                                      
// verify correct messages received                                                              
var resRegular = res["messages"][subRegular.str]["A"];                                                      
for(var i=0; i<6; i++)                                                                              
    assert.eq(resRegular[i]["count"], i);  

var resFilter = res["messages"][subFilter.str]["A"];                                                
for(var i=0; i<2; i++)                                                                              
    assert.eq(resFilter[i]["count"], i+4);  
