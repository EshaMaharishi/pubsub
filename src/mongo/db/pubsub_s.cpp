/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/pch.h"

#include <boost/thread.hpp>
#include <zmq.hpp>

#include "mongo/util/background.h"
#include "mongo/db/pubsub.h"
#include "mongo/db/pubsub_sendsock.h"
#include "mongo/s/mongos_options.h"

namespace mongo {

    class PubSubCleanup: public BackgroundJob {
    public:
        PubSubCleanup(){}
        virtual ~PubSubCleanup(){}

        virtual string name() const { return "PubSubCleanup"; }

        static string secondsExpireField;

        virtual void run() {

            PubSubSendSocket::extSendSocket = PubSub::initSendSocket();
            PubSub::extRecvSocket = PubSub::initRecvSocket();

            // error occurred while initializing sockets
            if (!pubsubEnabled)
                return;

            try {

                HostAndPort maxConfigHP;
                maxConfigHP.setPort(0);

                // connect to MAX PORT config server
                // TODO: hook into config server when mongos is added/removed like with repl sets
                std::vector<std::string> configServers = mongosGlobalParams.configdbs;
                for (std::vector<std::string>::iterator it = configServers.begin();
                     it != configServers.end();
                     it++) {
                        HostAndPort configHP = HostAndPort(*it);
                        if (configHP.port() > maxConfigHP.port())
                            maxConfigHP = configHP;

                        HostAndPort configPubEndpoint = HostAndPort(configHP.host(),
                                                                    configHP.port() + 2345);
                        PubSub::extRecvSocket->connect(("tcp://" +
                                                         configPubEndpoint.toString()).c_str());
                }

                HostAndPort configPullEndpoint = HostAndPort(maxConfigHP.host(),
                                                             maxConfigHP.port() + 1234);

                PubSubSendSocket::extSendSocket->connect(("tcp://" +
                                                 configPullEndpoint.toString()).c_str());

                // publishes to client subscribe sockets
                PubSub::intPubSocket.bind(PubSub::kIntPubSubEndpoint);
            }
            catch (zmq::error_t& e) {
                log() << "Error initializing PubSub sockets. Turning off PubSub..."
                      << causedBy(e);
                pubsubEnabled = false;
                publishDataEvents = false;
                return;
            }

            // proxy incoming messages to internal publisher to be received by clients
            boost::thread internalProxy(PubSub::proxy,
                                        PubSub::extRecvSocket,
                                        &PubSub::intPubSocket);

            // clean up subscriptions that have been inactive for at least 10 minutes
            boost::thread subscriptionCleanup(PubSub::subscriptionCleanup);

        }
    };

    void startPubsubBackgroundJob() {
        if (!pubsubEnabled)
            return;
        PubSubCleanup* pubSubCleanup = new PubSubCleanup();
        pubSubCleanup->go();
    }

}
