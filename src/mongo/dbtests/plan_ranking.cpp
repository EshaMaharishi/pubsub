/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

/**
 * This file tests db/query/plan_ranker.cpp and db/query/multi_plan_runner.cpp.
 */

#include "mongo/client/dbclientcursor.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/instance.h"
#include "mongo/db/json.h"
#include "mongo/db/query/multi_plan_runner.h"
#include "mongo/db/query/query_planner.h"
#include "mongo/db/query/query_planner_test_lib.h"
#include "mongo/db/query/stage_builder.h"
#include "mongo/dbtests/dbtests.h"

namespace mongo {

    // How we access the external setParameter testing bool.
    extern bool forceIntersectionPlans;

}  // namespace mongo

namespace PlanRankingTests {

    static const char* ns = "unittests.PlanRankingTests";

    class PlanRankingTestBase {
    public:
        PlanRankingTestBase() {
            Client::WriteContext ctx(ns);
            _client.dropCollection(ns);
        }

        void insert(const BSONObj& obj) {
            Client::WriteContext ctx(ns);
            _client.insert(ns, obj);
        }

        void addIndex(const BSONObj& obj) {
            Client::WriteContext ctx(ns);
            _client.ensureIndex(ns, obj);
        }

        /**
         * Use the MultiPlanRunner to pick the best plan for the query 'cq'.  Goes through
         * normal planning to generate solutions and feeds them to the MPR.
         *
         * Takes ownership of 'cq'.  Caller DOES NOT own the returned QuerySolution*.
         */
        QuerySolution* pickBestPlan(CanonicalQuery* cq) {
            Client::ReadContext ctx(ns);
            Collection* collection = ctx.ctx().db()->getCollection(ns);

            QueryPlannerParams plannerParams;
            plannerParams.options |= QueryPlannerParams::INDEX_INTERSECTION;

            // Fill out the available indices.
            IndexCatalog::IndexIterator ii = collection->getIndexCatalog()->getIndexIterator(false);
            while (ii.more()) {
                const IndexDescriptor* desc = ii.next();
                plannerParams.indices.push_back(IndexEntry(desc->keyPattern(),
                                                           desc->getAccessMethodName(),
                                                           desc->isMultikey(),
                                                           desc->isSparse(),
                                                           desc->indexName(),
                                                           desc->infoObj()));
            }

            // Plan.
            vector<QuerySolution*> solutions;
            Status status = QueryPlanner::plan(*cq, plannerParams, &solutions);
            ASSERT(status.isOK());

            // MPR requires >1 soln.
            ASSERT_GREATER_THAN(solutions.size(), 1U);

            // Fill out the MPR.
            _mpr.reset(new MultiPlanRunner(collection, cq));

            // Put each solution from the planner into the MPR.
            for (size_t i = 0; i < solutions.size(); ++i) {
                WorkingSet* ws;
                PlanStage* root;
                ASSERT(StageBuilder::build(*solutions[i], &root, &ws));
                // Takes ownership of all arguments.
                _mpr->addPlan(solutions[i], root, ws);
            }

            // And return a pointer to the best solution.  The MPR owns the pointer.
            size_t bestPlan = numeric_limits<size_t>::max();
            ASSERT(_mpr->pickBestPlan(&bestPlan));
            ASSERT_LESS_THAN(bestPlan, solutions.size());
            return solutions[bestPlan];
        }

    private:
        static DBDirectClient _client;
        scoped_ptr<MultiPlanRunner> _mpr;
    };

    DBDirectClient PlanRankingTestBase::_client;

    //
    // Test that the "prefer ixisect" parameter works.
    //
    class PlanRankingIntersectOverride : public PlanRankingTestBase {
    public:
        void run() {
            static const int N = 10000;

            // 'a' is very selective, 'b' is not.
            for (int i = 0; i < N; ++i) {
                insert(BSON("a" << i << "b" << 1));
            }

            // Add indices on 'a' and 'b'.
            addIndex(BSON("a" << 1));
            addIndex(BSON("b" << 1));

            // Run the query {a:4, b:1}.
            CanonicalQuery* cq;
            verify(CanonicalQuery::canonicalize(ns, BSON("a" << 100 << "b" << 1), &cq).isOK());
            ASSERT(NULL != cq);

            // {a:100} is super selective so choose that.
            // Takes ownership of cq.
            QuerySolution* soln = pickBestPlan(cq);
            ASSERT(QueryPlannerTestLib::solutionMatches(
                        "{fetch: {filter: {b:1}, node: {ixscan: {pattern: {a: 1}}}}}",
                        soln->root.get()));

            // Turn on the "force intersect" option.
            forceIntersectionPlans = true;

            // And run the same query again.
            ASSERT(CanonicalQuery::canonicalize(ns, BSON("a" << 100 << "b" << 1), &cq).isOK());

            // With the "ranking picks ixisect always" option we pick an intersection plan that uses
            // both the {a:1} and {b:1} indices even though it performs poorly.

            // Takes ownership of cq.
            soln = pickBestPlan(cq);
            ASSERT(QueryPlannerTestLib::solutionMatches(
                             "{fetch: {filter: null, node: {andSorted: {nodes: ["
                                     "{ixscan: {filter: null, pattern: {a:1}}},"
                                     "{ixscan: {filter: null, pattern: {b:1}}}]}}}}",
                             soln->root.get()));

            // Turn off the intersection forcing flag for future tests.
            forceIntersectionPlans = false;
        }
    };

    class All : public Suite {
    public:
        All() : Suite( "query_plan_ranking" ) {}

        void setupTests() {
            add<PlanRankingIntersectOverride>();
        }
    } planRankingAll;

}  // namespace PlanRankingTest
