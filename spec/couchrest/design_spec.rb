require File.expand_path("../../spec_helper", __FILE__)

describe CouchRest::Design do
  
  describe "defining a view" do
    it "should add a view to the design doc" do
      @des = CouchRest::Design.new
      method = @des.view_by :name
      method.should == "by_name"
      @des["views"]["by_name"].should_not be_nil
    end
  end
  
  describe "with an unsaved view" do
    before(:each) do
      @des = CouchRest::Design.new
      @des.view_by :name
    end
    it "should accept a name" do
      @des.name = "mytest"
      @des.name.should == "mytest"
    end
    it "should not save on view definition" do
      @des.rev.should be_nil
    end
    it "should freak out on view access" do
      lambda{@des.view :by_name}.should raise_error
    end
  end
  
  describe "saving" do
    before(:each) do
      @des = CouchRest::Design.new
      @des.view_by :name
      @des.database = reset_test_db!
    end
    it "should fail without a name" do
      lambda{@des.save}.should raise_error(ArgumentError)
    end
    it "should work with a name" do
      @des.name = "myview"
      @des.save
    end
  end
  
  describe "when it's saved" do
    before(:each) do
      @db = reset_test_db!
      @db.bulk_save([{"name" => "x"},{"name" => "y"}])
      @des = CouchRest::Design.new
      @des.database = @db
      @des.view_by :name
    end
    it "should by queryable when it's saved" do
      @des.name = "mydesign"
      @des.save
      res = @des.view :by_name
      res["rows"][0]["key"].should == "x"
    end
    it "should be queryable on specified database" do
      @des.name = "mydesign"
      @des.save
      @des.database = nil
      res = @des.view_on @db, :by_name
      res["rows"][0]["key"].should == "x"
    end
  end
  
  describe "from a saved document" do
    before(:each) do
      @db = reset_test_db!
      @db.save_doc({
        "_id" => "_design/test",
        "views" => {
          "by_name" => {
            "map" => "function(doc){if (doc.name) emit(doc.name, null)}"
          }
        }
      })
      @db.bulk_save([{"name" => "a"},{"name" => "b"}])
      @des = @db.get "_design/test"
    end
    it "should be a Design" do
      @des.should be_an_instance_of(CouchRest::Design)
    end
    it "should have a modifiable name" do
      @des.name.should == "test"
      @des.name = "supertest"
      @des.id.should == "_design/supertest"
    end
    it "should by queryable" do
      res = @des.view :by_name 
      res["rows"][0]["key"].should == "a"
    end
  end
  
  describe "a view with default options" do
    before(:all) do
      @db = reset_test_db!
      @des = CouchRest::Design.new
      @des.name = "test"
      @des.view_by :name, :descending => true
      @des.database = @db
      @des.save
      @db.bulk_save([{"name" => "a"},{"name" => "z"}])
    end
    it "should save them" do
      @d2 = @db.get(@des.id)
      @d2["views"]["by_name"]["couchrest-defaults"].should == {"descending"=>true}
    end
    it "should use them" do
      res = @des.view :by_name
      res["rows"].first["key"].should == "z"
    end
    it "should override them" do
      res = @des.view :by_name, :descending => false
      res["rows"].first["key"].should == "a"
    end
    it "should be able to get the original request" do
      res = @des.view :by_name
      res.raw_response.code.should == 200
      res.raw_response.headers[:content_type].should == 'text/plain;charset=utf-8'
      JSON.parse(res.raw_response).should == res
    end
  end
  
  describe "a view with multiple keys" do
    before(:all) do
      @db = reset_test_db!
      @des = CouchRest::Design.new
      @des.name = "test"
      @des.view_by :name, :age
      @des.database = @db
      @des.save
      @db.bulk_save([{"name" => "a", "age" => 2},
        {"name" => "a", "age" => 4},{"name" => "z", "age" => 9}])
    end
    it "should work" do
      res = @des.view :by_name_and_age
      res["rows"].first["key"].should == ["a",2]
    end
  end
  
  describe "a show function" do
    before(:all) do
      @db = reset_test_db!
      @des = CouchRest::Design.new
      @des.name = "test"
      @des.view_by :name
      @des.database = @db
      @des.show_using :echo
      @des.show_using :the_word_hi, "function(doc,req) { return { 'body' : 'hi' }; }"
      @des.show_using :transformed_document, "function(doc,req) { return {'body': doc['name'] + ' is ' + doc['age']}}"
      @des.save
      @thing1 = {"_id"=>"thing1", "name" => "a", "age" => 4}  
      @db.bulk_save([{"name" => "a", "age" => 2}, @thing1 ,{"name" => "z", "age" => 9}])
    end
    it "should return the document specified" do
      res = @des.show 'echo/thing1'
      @thing1.each_key do |k|
        res[k].should == @thing1[k]
      end
    end
    it "should return the document transformed" do
      res = @des.show 'transformed_document/thing1'
      res.should == 'a is 4'
    end
    it "should return something arbitirary" do
      res = @des.show 'the_word_hi/thing1'
      res.should == 'hi'
    end
  end
    
  describe "a list function" do
    before(:all) do
      @db = reset_test_db!
      @des = CouchRest::Design.new
      @des.name = "test"
      @des.view_by :name
      @des.database = @db
      @des.list_using :echo
      
      @des.list_using :the_word_hi, "function(head,req) { send('hi'); }"
      
      @des.save
      @db.bulk_save([{"name" => "a", "age" => 2},
        {"name" => "a", "age" => 4},{"name" => "z", "age" => 9}])
        
    end
    it "should return the same rows as the view, with a header" do   
      res = @des.list 'echo/by_name'
      res.raw_response.headers[:content_type] == "application/json"
      res2 = @des.view :by_name
      res.should == res2['rows']
    end
    
    it "should return anything you want" do
      res = @des.list 'the_word_hi/by_name'
      res.should == 'hi'
    end
    
    it "should support listing a view in a different design doc" do
      @des2 = CouchRest::Design.new
      @des2.name = "other"
      @des2.view_by :age
      @des2.database = @db
      @des2.save
      
      res = @des.list 'echo/other/by_age'
      res.should == @des2.view(:by_age)['rows']
    end
    
  end

  describe "a view with nil and 0 values" do
    before(:all) do
      @db = reset_test_db!
      @des = CouchRest::Design.new
      @des.name = "test"
      @des.view_by :code
      @des.database = @db
      @des.save
      @db.bulk_save([{"code" => "a", "age" => 2},
        {"code" => nil, "age" => 4},{"code" => 0, "age" => 9}])
    end
    it "should work" do
      res = @des.view :by_code
      res["rows"][0]["key"].should == 0
      res["rows"][1]["key"].should == "a"
      res["rows"][2].should be_nil
    end
  end


end
