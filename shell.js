DB.prototype.nvPrintSharding = function(){
  print('ok');
}

_nv_db_original_help = DB.prototype.help;
DB.prototype.help = function(){
  _nv_db_original_help.apply(this);
  print("\tdb.nvPrintSharding() - print ok");  
}