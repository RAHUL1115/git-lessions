console.log('---------------------------- ITERATING ------------------------------------');

// running 10LAC loop
let jsonOBJ = {};
let limit = 1000000
let lastKey = '';
console.time('test1')
console.timeLog('test1',"start")
for (let i = 0; i < limit; i++) {   
    jsonOBJ[i+'key'] = i;
    if(i == limit-1){
        lastKey = i+'key'
    }
}
console.timeEnd('test1')

console.log('\n---------------------------- JSON SEARCH ------------------------------------');

console.time('test2')
console.timeLog('test2',"start")
console.log(jsonOBJ[lastKey]);
console.timeEnd('test2')

console.log('\n---------------------------- ITERATING ------------------------------------');

console.time('test3')
console.timeLog('test3',"start")
for (const [key,value] of Object.entries(jsonOBJ)) {
    if(key==lastKey){
        console.log(value);
        console.timeEnd('test3');
        break;
    }
}