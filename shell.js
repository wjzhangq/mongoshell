nv = {
  helpItem:{'db':{}, 'coll':{}},
  extend:function(obj, funcName, func, funcHelp){
    obj.prototype[funcName] = func;
    if (typeof(funcHelp) == 'undefined'){
      funcHelp = funcName + "() - No help";
    }
    
    if (obj === DB){
        nv.helpItem.db[funcName] = funcHelp;
        obj.prototype.nvHelp = nv.dbHelp;
    }else if (obj === DBCollection){
        nv.helpItem.coll[funcName] = funcHelp;
        obj.prototype.nvHelp = nv.collHelp;
    }
  },
  dbHelp:function(){
    for(var i in nv.helpItem.db){
      print("\tdb." + nv.helpItem.db[i]);
    }
  },
  collHelp:function(){
    var shortName = this.getName();
    for(var i in nv.helpItem.coll){
      print("\tdb." + shortName + "." + nv.helpItem.coll[i]);
    }
  }
};

//for db.nvBalancer detail:http://wiki.zhangwenjin.com/doku.php?id=youku:mongo:shelljs#dbnvbalancer
nv.nvBalancer = function(status){  
  conf = db.getSisterDB("config");
  var balancer_not_find = true;
  var balancer_stopped = false;
  var ret = conf.settings.find({_id:'balancer'}, {'stopped':1}).forEach(function(item){
    balancer_not_find = false;
    balancer_stopped = item.stopped;
  });
  //if not found insert one
  if (balancer_not_find){
    conf.settings.insert({"_id":"balancer", "stopped":false});
  }
  
  var balancer_status = balancer_stopped ? 'off' : 'on';

  if (typeof(status) != 'undefined'){
    status = !!status;
    if (status == balancer_stopped){
      print('Balancer is ' + balancer_status +' before');
      balancer_stopped = !balancer_stopped;
      conf.settings.update({_id:'balancer'}, {$set:{stopped:balancer_stopped}});
      balancer_status = balancer_stopped ? 'off' : 'on';
    }
  }
  print('Balancer is ' + balancer_status +' now');
}

nv.nvPrintShards = function(){
  var shards = nv._listShards();
  print(str_pad('No.', 5, ' ') + str_pad('name', 30, ' ') + str_pad('hostname', 30, ' ') + str_pad('host', 20, ' '));
  for(var i = 0; i < shards.length; i++){
    print(str_pad(i, 5, ' ') + str_pad(shards[i].name, 30, ' ') + str_pad(shards[i].hostname, 30, ' ') + shards[i].host.join(','));
  }
}

//coll
nv.nvPrintChunks = function(){
  var coll = this;
  var collName = coll.getName();
  var dbName = coll.getDB().getName();
  
  nv._listChunks(dbName + "." + collName);
}

nv.nvSplitChunk = function(middle){
  var coll = this;
  var collName = coll.getName();
  var dbName = coll.getDB().getName();
  nv._splitChunk(dbName + "." + collName, middle);
}

nv.nvMoveChunk = function(find, to){
  var coll = this;
  var collName = coll.getName();
  var dbName = coll.getDB().getName();
  nv._moveChunk(dbName + "." + collName, find, to);
}










//utils
nv._getShardKey = function (dbName, collName){
  var conf = db.getSisterDB("config");
  var chunkinfo = conf.chunks.find({ns:dbName + '.' + collName},{min:1}).limit(1).toArray().pop();
  var shardKey = '';
  if (chunkinfo){
    for(var i in chunkinfo.min){
      shardKey = i;
      break;
    }
  }
  return shardKey;
}

nv._listChunks = function(ns){
  var tmp = ns.split('.');
  if (tmp.length !== 2){
    print('ns ' + ns + ' is invalid!');
    return;
  }
  var dbName = tmp[0];
  var collName = tmp[1];
  
  var conf = db.getSisterDB("config");
  var myDb = db.getSisterDB(dbName);
  var coll = myDb[collName];
  
  var chunkSet = {};
  var shardKey = nv._getShardKey(dbName, collName);
  if (!shardKey){
    print('Shard Key not found!');
    return;
  }
  conf.chunks.find({ns:dbName + '.' + collName},{_id:1,min:1,max:1, shard:1}).sort({min:1}).forEach(function(item){
   if (typeof(chunkSet[item.shard]) === 'undefined'){
     chunkSet[item.shard] = [];
   }
   if(typeof(item.min[shardKey]) != "object" && typeof(item.max[shardKey]) != "object"){
     chunkSet[item.shard].push([item.min[shardKey], item.max[shardKey]]);
   }
  });
  
  for (var i in chunkSet){
    var shard = i;
    var chunks = chunkSet[i];
    print('Shard:' + shard);
    print("\t" + str_pad('min', 12, ' ') + str_pad('max', 12, ' ') + str_pad('count', 12, ' ') + str_pad('time', 12, ' '));
    for(var j in chunks){
      var start = new Date().getTime();
      var count = 0;
      var param = {};
      param[shardKey] = {$gte:chunks[j][0], $lt:chunks[j][1]};
      try{
        count = coll.find(param).count();
      }catch(e){
        printjson(e)
      }
      var speed = (new Date().getTime() - start) / 1000;
      print("\t" + str_pad(chunks[j][0], 12, ' ') + str_pad(chunks[j][1], 12, ' ') + str_pad(count, 12, ' ') + str_pad(speed, 12, ' '));
    }
  }
}

nv._splitChunk = function(ns, middle){
  var tmp = ns.split('.');
  if (tmp.length !== 2){
    print('ns ' + ns + ' is invalid!');
    return;
  }
  var dbName = tmp[0];
  var collName = tmp[1];
  
  var shardKey = nv._getShardKey(dbName, collName);
  if (!shardKey){
    print('Shard Key not found!');
    return;
  }
  
  var cond = {};
  cond[shardKey] = 0;
  var myadmin = db.getSisterDB("admin");
  
  if (typeof(middle) == 'object'){
    for(var i = 0; i < middle.length; i++){
      cond[shardKey] = middle[i];
      printjson(cond);
      try{
        myadmin.runCommand({split:ns, middle:cond});
      }catch(e){
        printjson(e);
      }
    }
  }else{
    cond[shardKey] = middle;
    printjson(cond);
    try{
      myadmin.runCommand({split:ns, middle:cond});
    }catch(e){
      printjson(e);
    }
  }
}

nv._moveChunk = function(ns, find, to){
  var tmp = ns.split('.');
  if (tmp.length !== 2){
    print('ns ' + ns + ' is invalid!');
    return;
  }
  var dbName = tmp[0];
  var collName = tmp[1];
  
  var shardKey = nv._getShardKey(dbName, collName);
  if (!shardKey){
    print('Shard Key not found!');
    return;
  }
  
  var shards = nv._listShards();
  var toShard = '';
  
  
  
  if (typeof(to) == 'number'){
    if (to < 0 || to >= shards.length){
      print('to No must in 0 to ' + (shards.length -1));
      return;
    }
    toShard = shards[to].name;
  }else{
    var toShardOk = false;
    shards.forEach(function(item){
      if (item.name == to){
        toShardOk = true;
      }
    });
    if (toShardOk == false){
      print('shard name "' + to + '" is invalid!');
      return;
    }
    toShard = to;
  }
  
  var cmd = {moveChunk:ns, find:{}, to:toShard};
  cmd.find[shardKey] = 0;
  var myadmin = db.getSisterDB("admin");
  
  if (typeof(find) == 'object'){
    for(var i = 0; i < find.length; i++){
      cmd.find[shardKey] = middle[i];
      printjson(cmd);
      try{
        myadmin.runCommand(cmd);
      }catch(e){
        printjson(e);
      }
    }
  }else{
    cmd.find[shardKey] = find;
    printjson(cmd);
    try{
      myadmin.runCommand(cmd);
    }catch(e){
      printjson(e);
    }
  }
  
}

nv._listShards = function(){
  var myconf = db.getSisterDB("config");
  
  var shards = [];
  myconf.shards.find().sort({'_id':1}).forEach(function(item){
    var tmp1 = item.host.split('/', 2);
    var tmp2 = tmp1[1].split(',', 2);
    var shard = {'name':item._id, 'hostname':tmp1[0], 'host':tmp2};
    shards.push(shard);
  });
  
  return shards;
}

//extend utils
function str_pad (input, pad_length, pad_string, pad_type) {
    // Returns input string padded on the left or right to specified length with pad_string  
    // 
    // version: 1103.1210
    // discuss at: http://phpjs.org/functions/str_pad
    // +   original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
    // + namespaced by: Michael White (http://getsprink.com)
    // +      input by: Marco van Oort
    // +   bugfixed by: Brett Zamir (http://brett-zamir.me)
    // *     example 1: str_pad('Kevin van Zonneveld', 30, '-=', 'STR_PAD_LEFT');
    // *     returns 1: '-=-=-=-=-=-Kevin van Zonneveld'
    // *     example 2: str_pad('Kevin van Zonneveld', 30, '-', 'STR_PAD_BOTH');
    // *     returns 2: '------Kevin van Zonneveld-----'
    var half = '',
        pad_to_go;
 
    var str_pad_repeater = function (s, len) {
        var collect = '',
            i;
 
        while (collect.length < len) {
            collect += s;
        }
        collect = collect.substr(0, len);
 
        return collect;
    };
 
    input += '';
    pad_string = pad_string !== undefined ? pad_string : ' ';
 
    if (pad_type != 'STR_PAD_LEFT' && pad_type != 'STR_PAD_RIGHT' && pad_type != 'STR_PAD_BOTH') {
        pad_type = 'STR_PAD_RIGHT';
    }
    if ((pad_to_go = pad_length - input.length) > 0) {
        if (pad_type == 'STR_PAD_LEFT') {
            input = str_pad_repeater(pad_string, pad_to_go) + input;
        } else if (pad_type == 'STR_PAD_RIGHT') {
            input = input + str_pad_repeater(pad_string, pad_to_go);
        } else if (pad_type == 'STR_PAD_BOTH') {
            half = str_pad_repeater(pad_string, Math.ceil(pad_to_go / 2));
            input = half + input + half;
            input = input.substr(0, pad_length);
        }
    }
 
    return input;
}

function range (low, high, step) {
    // http://kevin.vanzonneveld.net
    // +   original by: Waldo Malqui Silva
    // *     example 1: range ( 0, 12 );
    // *     returns 1: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    // *     example 2: range( 0, 100, 10 );
    // *     returns 2: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    // *     example 3: range( 'a', 'i' );
    // *     returns 3: ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']
    // *     example 4: range( 'c', 'a' );
    // *     returns 4: ['c', 'b', 'a']
    low = tryToNumber(low);
    high = tryToNumber(high);
    step = tryToNumber(step);
    
    var matrix = [];
    var inival, endval, plus;
    var walker = step || 1;
    var chars = false;

    if (!isNaN(low) && !isNaN(high)) {
        inival = low;
        endval = high;
    } else if (isNaN(low) && isNaN(high)) {
        chars = true;
        inival = low.charCodeAt(0);
        endval = high.charCodeAt(0);
    } else {
        inival = (isNaN(low) ? 0 : low);
        endval = (isNaN(high) ? 0 : high);
    }

    plus = ((inival > endval) ? false : true);
    if (plus) {
        while (inival <= endval) {
            matrix.push(((chars) ? String.fromCharCode(inival) : inival));
            inival += walker;
        }
    } else {
        while (inival >= endval) {
            matrix.push(((chars) ? String.fromCharCode(inival) : inival));
            inival -= walker;
        }
    }

    return matrix;
}

function tryToNumber(str){
  var map = {'k':1000, 'w':10000}
  var ret = str;
  if (typeof(str) == 'string'){
    var num = parseFloat(str, 10);
    strP = /^\d+(\.\d+)?$/;
    if (strP.test(str)){
      ret = num;
    }else if ((!isNaN(num)) && num != 0){
      var endUnit = str.charAt(str.length - 1).toLowerCase();
      for(var i in map){
        if (endUnit === i){
          ret = num * map[i];
          break;
        }
      }
    }
  }
  return ret;
}




nv.extend(DB, 'nvBalancer', nv.nvBalancer, 'nvBalancer([status]) - get or set balancer status, status value is 1 or 0');
nv.extend(DB, 'nvPrintShards', nv.nvPrintShards, 'nvPrintShards() - Print Shard from confid.shard');
nv.extend(DB, 'nvPrintChunks', nv._listChunks, 'nvPrintChunks(ns) - Print collection chunk info, include record count, size and speed time');
nv.extend(DB, 'nvSplitChunk', nv._splitChunk, 'nvSplitChunk(ns, middle) - split ns by middle, middle maybe a array or integer');
nv.extend(DB, 'nvMoveChunk', nv._moveChunk, 'nvMoveChunk(ns, find, to) - move chunk, find may be an array or integer');
nv.extend(DBCollection, 'nvPrintChunks', nv.nvPrintChunks, 'nvPrintChunks() - Print collection chunk info, include record count, size and speed time');
nv.extend(DBCollection, 'nvSplitChunk', nv.nvSplitChunk, 'nvSplitChunk(middle) - split ns by middle, middle maybe a array or integer');
nv.extend(DBCollection, 'nvMoveChunk', nv.nvMoveChunk, 'nvMoveChunk(find, to) - move chunk, find may be an array or integer');

