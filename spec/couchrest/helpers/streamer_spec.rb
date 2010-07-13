require File.expand_path("../../../spec_helper", __FILE__)

describe CouchRest::Streamer do
  before(:all) do
    @cr = CouchRest.new(COUCHHOST)
    @db = @cr.database(TESTDB)
    @db.delete! rescue nil
    @db = @cr.create_db(TESTDB) rescue nil
    @streamer = CouchRest::Streamer.new(@db)
    @docs = (1..1000).collect{|i| {:integer => i, :string => i.to_s}}
    @db.bulk_save(@docs)
    @db.save_doc({
      "_id" => "_design/first",
      :views => {
        :test => {
	        :map => "function(doc){for(var w in doc){ if(!w.match(/^_/))emit(w,doc[w])}}"
        },
        :by_integer => {
          :map => "function(doc){ emit(doc.integer); }"
        }
      },
      :lists => {
        :keystring => <<-JAVASCRIPT
function(head,req) {
   var row; 
   var inc = 1;
   while(row = getRow()) { 
     if (inc % 10 == 0) {
       send(row.key + ' ' + inc)
       send('\\n')
     }
    inc++;
   }
};
JAVASCRIPT
      },
      :shows => {
        :valuestring => <<-JAVASCRIPT
function (doc,req) {
  send("The value is:\\n");
  send(doc.integer);
}
JAVASCRIPT
      }
    })
  end
  
  
  it "should yield each row in a view" do
    count = 0
    sum = 0
    @streamer.view("_all_docs") do |row|
      count += 1
    end
    count.should == 1001
  end

  it "should accept several params" do
    count = 0
    @streamer.view("_design/first/_view/test", :include_docs => true, :limit => 5) do |row|
      count += 1
    end
    count.should == 5
  end

  it "should accept both view formats" do
    count = 0
    @streamer.view("_design/first/_view/test") do |row|
      count += 1
    end
    count.should == 2000
    count = 0
    @streamer.view("first/test") do |row|
      count += 1
    end
    count.should == 2000
  end


  it "should yield each line in a list" do
    count = 0
    @streamer.list("first/keystring/test") do |row|
      count += 1
    end
    count.should == 200
  end
  
  it "should yield each line in a show" do
    count = 0
    
    @res = @db.view 'first/by_integer', {:startkey => 1, :endkey => 1}
    id = @res['rows'].first['id']
    
    rows = []
    @streamer.show("first/valuestring/#{id}") do |row|
      rows << row
    end
    
    rows[0].should == "The value is:\n"
    rows[1].should == "1"
  end
  
end
