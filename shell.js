nv = {
  helpItem:{'db':{}, 'coll':{}},
  original_help:{},
  extend:function(obj, funcName, func, funcHelp){
    obj.prototype[funcName] = func;
    if (typeof(funcHelp) == 'undefined'){
      funcHelp = funcName + "() - No help";
    }
    
    if (obj === DB){
      nv.helpItem.db[funcName] = funcHelp;
      if (obj.prototype.help !== nv.dbHelp){
        nv.dbOriginalHelp = obj.prototype.help;
        obj.prototype.help = nv.dbHelp;
      }
    }else if (obj === DBCollection){
      nv.helpItem.coll[funcName] = funcHelp;
      if (obj.prototype.help !== nv.collOriginalHelp){
        nv.collOriginalHelp = obj.prototype.help;
        obj.prototype.help = nv.collHelp;
      }
    }
  },
  dbHelp:function(){
    if (typeof(nv.dbOriginalHelp) == 'function'){
      nv.dbOriginalHelp.apply(this);
    }
    for(var i in nv.helpItem.db){
      print("\tdb." + nv.helpItem.db[i]);
    }
  },
  collHelp:function(){
    if (typeof(nv.collOriginalHelp) == 'function'){
      nv.collOriginalHelp.apply(this);
    }
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

nv._listChunks = function(dbName, collName){
  var conf = db.getSisterDB("config");
  var myDb = db.getSisterDB(dbName);
  var coll = myDb[collName];
  
  var chunkSet = {};
  var shardKey = '';
  conf.chunks.find({ns:dbName + '.' + collName},{_id:1,min:1,max:1, shard:1}).sort({min:1}).forEach(function(item){
   if (shardKey === ''){
     for(var i in item.min){
       shardKey = i;
       break;
     }
   }
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

nv.nvPrintChunks = function(){
  var coll = this;
  var collName = coll.getName();
  var dbName = coll.getDB().getName();
  
  nv._listChunks(dbName, collName);
}

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




nv.extend(DB, 'nvBalancer', nv.nvBalancer, 'nvBalancer([status]) - get or set balancer status, status value is 1 or 0');
nv.extend(DB, 'nvPrintChunks', nv._listChunks, 'nvPrintChunks(dbName, collName) - Print collection chunk info, include record count, size and speed time');
nv.extend(DBCollection, 'nvPrintChunks', nv.nvPrintChunks, 'nvPrintChunks() - Print collection chunk info, include record count, size and speed time');

