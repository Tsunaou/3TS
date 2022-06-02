#include <cstdint>
#include <iostream>
#include <vector>
 
#include <bsoncxx/builder/stream/document.hpp> 
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::to_json;

using namespace mongocxx;
 
int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    //mongocxx::instance inst{};
    instance inst;
    //mongocxx::client client{mongocxx::uri{"mongodb://9.134.39.34:27017/admin"}};
    client client{mongocxx::uri{"mongodb://9.134.39.34:27037/admin"}};
    write_concern wc_majority{};
    wc_majority.acknowledge_level(write_concern::level::k_majority);

    read_concern rc_snapshot{};
    rc_snapshot.acknowledge_level(read_concern::level::k_snapshot);

    read_preference rp_primary{};
    rp_primary.mode(read_preference::read_mode::k_primary);

    auto db = client["testdb"];

    auto coll_old1 = client["testdb"]["coll1"];
    auto coll_old2 = client["testdb"]["coll2"];
    coll_old1.drop(wc_majority);
    coll_old2.drop(wc_majority);

    auto coll1 = client["testdb"]["coll1"];
    auto coll2 = client["testdb"]["coll2"];

    options::transaction opts;
    opts.write_concern(wc_majority);
    opts.read_concern(rc_snapshot);
    opts.read_preference(rp_primary);

    auto session1 = client.start_session();
    session1.start_transaction(opts);
    std::cout << "session1 start tx " << to_json(session1.id()) << std::endl;

    auto session2 = client.start_session();
    session2.start_transaction(opts);
    std::cout << "session2 start tx " << to_json(session2.id()) << std::endl;

    auto doc_value1 = document{} << "k" << "0" << "v" << "0" << finalize;
    auto doc_value2 = document{} << "k" << "1" << "v" << "0" << finalize;

    coll1.insert_one(session1, doc_value1.view());
    std::cout << "session1 insert k:0 v:0" << std::endl;
    session1.commit_transaction();
    std::cout << "session1 commit" << std::endl;

    //coll2.insert_one(session2, doc_value2.view());

    auto cursor = coll1.find(session2, document{} << "k" << "0" << finalize);

    for (auto&& doc : cursor) {
        std::cout << "doc:" + to_json(doc) << std::endl;
    }

    std::cout << "------------ Test for predicate-based operations ------------------" << std::endl;
    auto session3 = client.start_session();
    mongocxx::collection test = client["testdb"]["t1"];

    // test select where; predicate k>0 and k<2
    // { 
    //     "$and" : [
    //         { 
    //             "k" : { 
    //                 "$gt" : "0"
    //             }
    //         }, 
    //         { 
    //             "k" : { 
    //                 "$lt" : "2"
    //             }
    //         }
    //     ]
    // }
    
    document builder{};

    auto filter = builder << "$and" << open_array;
    filter = filter << open_document << "k" << open_document << "$gt" << "0" << close_document << close_document;
    filter = filter << open_document << "k" << open_document << "$lt" << "2" << close_document << close_document;
    auto after_filter = filter << close_array;
    bsoncxx::document::value doc_value = after_filter << finalize;
    cursor = test.find(session3, doc_value.view());

    for(auto doc : cursor) {
        std::cout << to_json(doc) << "\n";
    }

    // cursor = test.find(
    //     session3,
    //     {}
    // );

    // for(auto doc : cursor) {
    //     std::cout << to_json(doc) << "\n";
    // }

    // test delete where
    
    
    // test update where
}
