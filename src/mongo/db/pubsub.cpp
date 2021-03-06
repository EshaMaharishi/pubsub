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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/pch.h"

#include "mongo/db/pubsub.h"

#include <time.h>
#include <zmq.hpp>

#include "mongo/db/pubsub_sendsock.h"
#include "mongo/db/server_options_helpers.h"
#include "mongo/db/server_parameters.h"

namespace mongo {

    MONGO_EXPORT_SERVER_PARAMETER(useDebugTimeout, bool, false);

    namespace {
        // used as a timeout for polling and cleaning up inactive subscriptions
        long maxTimeoutMillis = 1000 * 60 * 10;
    }

    SubscriptionMessage::SubscriptionMessage(SubscriptionId _subscriptionId,
                                             std::string _channel,
                                             BSONObj _message,
                                             unsigned long long _timestamp) {
        subscriptionId = _subscriptionId;
        channel = _channel;
        message = _message;
        timestamp = _timestamp;
    }

    bool operator<(const SubscriptionMessage& m1, const SubscriptionMessage& m2) {
        if (m1.subscriptionId < m2.subscriptionId)
            return true;
        if (m1.subscriptionId == m2.subscriptionId && m1.channel < m2.channel)
            return true;
        if (m1.subscriptionId == m2.subscriptionId &&
            m1.channel == m2.channel &&
            m1.timestamp > m2.timestamp)
            return true;
        return false;
    }


    /**
     * Sockets for internal communication across replsets and clusters.
     *
     * Mongods in a replica set communicate directly, while mongoses in a cluster
     * communicate through the config server.
     * Further, mongod's "publish" information to each other, whereas mongoses
     * "push" information to a queue shared between the 3 config servers.
     */

    const char* const PubSub::kIntPubSubEndpoint = "inproc://pubsub";

    zmq::context_t PubSub::zmqContext(1);
    zmq::socket_t PubSub::intPubSocket(zmqContext, ZMQ_PUB);
    zmq::socket_t* PubSub::extRecvSocket = NULL;

    zmq::socket_t* PubSub::initSendSocket() {
        zmq::socket_t* sendSocket = NULL;
        try {
            sendSocket = new zmq::socket_t(zmqContext, isMongos() ? ZMQ_PUSH : ZMQ_PUB);
            int hwm = 0;
            sendSocket->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm)); 
        }
        catch (zmq::error_t& e) {
            log() << "Error initializing zmq send socket for PubSub." << causedBy(e);
            pubsubEnabled = false;
            publishDataEvents = false;
            return NULL;
        }
        return sendSocket;
    }

    zmq::socket_t* PubSub::initRecvSocket() {
        zmq::socket_t* recvSocket = NULL;
        try {
            recvSocket =
                new zmq::socket_t(zmqContext, serverGlobalParams.configsvr ? ZMQ_PULL : ZMQ_SUB);
            // config server uses pull socket, so cannot set subscribe option
            if (!serverGlobalParams.configsvr) {
                recvSocket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
            }
        }
        catch (zmq::error_t& e) {
            log() << "Error initializing zmq recv socket for PubSub." << causedBy(e);
            pubsubEnabled = false;
            publishDataEvents = false;
            return NULL;
        }
        return recvSocket;
    }

    void PubSub::proxy(zmq::socket_t* subscriber, zmq::socket_t* publisher) {
        try {
            zmq::proxy(*subscriber, *publisher, NULL);
        }
        catch (zmq::error_t& e) {
            log() << "Error starting zmq proxy for PubSub." << causedBy(e);
            pubsubEnabled = false;
            publishDataEvents = false;
        }
    }

    // runs in a background thread and cleans up subscriptions
    // that have not been polled in at least 10 minutes
    void PubSub::subscriptionCleanup() {
        if (useDebugTimeout)
            // change timeout to 100 millis for testing
            maxTimeoutMillis = 100;
        while (true) {
            {
                SimpleMutex::scoped_lock lk(mapMutex);
                for (SubscriptionMap::iterator it = subscriptions.begin();
                     it != subscriptions.end();
                     it++) {
                        shared_ptr<SubscriptionInfo> s = it->second;
                        if (s->polledRecently) {
                            s->polledRecently = 0;
                        }
                        else {
                            try {
                                s->sock->close();
                            }
                            catch (zmq::error_t& e) {
                                log() << "Error closing zmq socket." << causedBy(e);
                            }
                            subscriptions.erase(it);
                        }
                }
            }
            sleepmillis(maxTimeoutMillis);
        }
    }

    /**
     * In-memory data structures for pubsub.
     * Subscribers can poll for more messages on their subscribed channels, and the class
     * keeps an in-memory map of the id (cursor) they are polling on to the actual
     * poller instance used to retrieve their messages.
     *
     * The map is wrapped in a class to facilitate clean (multi-threaded) access
     * to the table from subscribe (to add entries), unsubscribe (to remove entries),
     * and poll (to access entries) without exposing any locking mechanisms
     * to the user.
     */

    PubSub::SubscriptionMap PubSub::subscriptions;

    const long PubSub::maxPollInterval = 1000; // milliseconds

    SimpleMutex PubSub::mapMutex("subsmap");

    // Outwards-facing interface for PubSub across replica sets and sharded clusters

    // TODO: add secure access to this channel?
    // perhaps return an <oid, key> pair?
    SubscriptionId PubSub::subscribe(const std::string& channel,
                                     const BSONObj& filter,
                                     const BSONObj& projection) {
        SubscriptionId subscriptionId;
        subscriptionId.init();

        zmq::socket_t* subSocket;
        try {
            subSocket = new zmq::socket_t(zmqContext, ZMQ_SUB);
            subSocket->setsockopt(ZMQ_SUBSCRIBE, channel.c_str(), channel.length());
            int hwm = 0;
            subSocket->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm)); 
            subSocket->connect(PubSub::kIntPubSubEndpoint);
        }
        catch (zmq::error_t& e) {
            uassert(18539, e.what(), false);
        }

        shared_ptr<SubscriptionInfo> s(new SubscriptionInfo());
        s->sock.reset(subSocket);
        s->inUse = 0;
        s->shouldUnsub = 0;
        s->polledRecently = 1;

        s->filter.reset(NULL);
        if (!filter.isEmpty())
            s->filter.reset(new Matcher2(filter.getOwned()));

        s->projection.reset(NULL);
        if (!projection.isEmpty()){
            s->projection.reset(new Projection());
            s->projection->init(projection);
        }

        SimpleMutex::scoped_lock lk(mapMutex);
        subscriptions.insert(std::make_pair(subscriptionId, s));

        return subscriptionId;
    }

    std::priority_queue<SubscriptionMessage> PubSub::poll(
            std::set<SubscriptionId>& subscriptionIds,
            long timeout, long long& millisPolled,
            bool& pollAgain,
            std::map<SubscriptionId, std::string>& errors) {

        std::priority_queue<SubscriptionMessage> messages;
        SubscriptionVector subs;
        std::vector<zmq::pollitem_t> items;

        PubSub::getSubscriptions(subscriptionIds, items, subs, errors);

        // if there are no valid subscriptions to check, return. there may have
        // been errors during getSubscriptions which will be returned.
        if (items.size() == 0)
            return messages;

        // limit time polled to ten minutes.
        // poll for max poll interval or timeout, whichever is shorter, until
        // client timeout has passed
        if (timeout > maxTimeoutMillis || timeout < 0)
            timeout = maxTimeoutMillis;
        long long pollRuntime = 0LL;
        long currPollInterval = std::min(PubSub::maxPollInterval, timeout);

        try {
            // while no messages have been received on any of the subscriptions,
            // continue polling coming up for air in intervals to check if any of the
            // subscriptions have been canceled
            while (timeout > 0 && !zmq::poll(&items[0], items.size(), currPollInterval)) {
                for (size_t i = 0; i < subs.size(); i++) {
                    if (subs[i].second->shouldUnsub) {
                        SubscriptionId subscriptionId = subs[i].first;
                        errors.insert(std::make_pair(subscriptionId,
                                                     "Poll interrupted by unsubscribe."));
                        items.erase(items.begin() + i);
                        subs.erase(subs.begin() + i);
                        PubSub::unsubscribe(subscriptionId, errors, true);
                        i--;
                    }
                }

                // If all sockets that were polling are unsubscribed, return
                if (items.size() == 0) {
                    millisPolled = pollRuntime;
                    return messages;
                }

                timeout -= currPollInterval;
                pollRuntime += currPollInterval;

                // stop polling if poll has run longer than the max timeout (default ten minutes,
                // or 100 millis if debug flag is set)
                if (pollRuntime >= maxTimeoutMillis) {
                    millisPolled = pollRuntime;
                    pollAgain = true;
                    endCurrentPolls(subs);
                    return messages;
                }

                currPollInterval = std::min(PubSub::maxPollInterval, timeout);
            }
        }
        catch (zmq::error_t& e) {
            log() << "Error polling on zmq socket." << causedBy(e);
            // check all sockets back in
            endCurrentPolls(subs);
            uassert(18547, e.what(), false);
        }

        // if we reach this point, then we know at least 1 message
        // has been received on some subscription
        messages = PubSub::recvMessages(subs, errors);

        millisPolled = pollRuntime;
        return messages;
    }

    void PubSub::endCurrentPolls(SubscriptionVector& subs) {
        for (SubscriptionVector::iterator subIt = subs.begin(); subIt != subs.end(); subIt++) {
            shared_ptr<SubscriptionInfo> s = subIt->second;
            PubSub::checkinSocket(s);
        }
    }

    void PubSub::getSubscriptions(std::set<SubscriptionId>& subscriptionIds,
                                  std::vector<zmq::pollitem_t>& items,
                                  SubscriptionVector& subs,
                                  std::map<SubscriptionId, std::string>& errors) {
        // check if each oid is for a valid subscription.
        // for each oid already in an active poll, set an error message
        // for each non-active oid, set active poll and create a poller
        for (std::set<SubscriptionId>::iterator it = subscriptionIds.begin();
             it != subscriptionIds.end();
             it++) {

                SubscriptionId subscriptionId = *it;
                std::string errmsg;
                shared_ptr<SubscriptionInfo> s = PubSub::checkoutSocket(subscriptionId, errmsg);

                if (s == NULL) {
                    errors.insert(std::make_pair(subscriptionId, errmsg));
                    continue;
                }

                // create subscription info and poll item for socket.
                // items and subs have corresponding info at the same indexes
                zmq::pollitem_t pollItem = { *(s->sock), 0, ZMQ_POLLIN, 0 };
                items.push_back(pollItem);
                subs.push_back(std::make_pair(subscriptionId, s));
        }
    }

    shared_ptr<PubSub::SubscriptionInfo> PubSub::checkoutSocket(SubscriptionId subscriptionId,
                                                                std::string& errmsg) {
        SimpleMutex::scoped_lock lk(mapMutex);
        SubscriptionMap::iterator subIt = subscriptions.find(subscriptionId);

        if (subIt == subscriptions.end() || subIt->second->shouldUnsub) {
            errmsg = "Subscription not found.";
            return shared_ptr<SubscriptionInfo>();
        }
        else if (subIt->second->inUse) {
            errmsg = "Poll currently active.";
            return shared_ptr<SubscriptionInfo>();
        }
        else {
            shared_ptr<SubscriptionInfo> s = subIt->second;
            s->inUse = 1;
            return s;
        }
    }

    void PubSub::checkinSocket(shared_ptr<SubscriptionInfo> s) {
        s->polledRecently = 1;
        s->inUse = 0;
    }

    std::priority_queue<SubscriptionMessage> PubSub::recvMessages(
                                SubscriptionVector& subs,
                                std::map<SubscriptionId, std::string>& errors) {

        std::priority_queue<SubscriptionMessage> outbox;

        for (SubscriptionVector::iterator subIt = subs.begin(); subIt != subs.end(); subIt++) {
            SubscriptionId subscriptionId = subIt->first;
            shared_ptr<SubscriptionInfo> s = subIt->second;

            try {
                zmq::message_t msg;
                while (s->sock->recv(&msg, ZMQ_DONTWAIT)) {
                    // receive channel
                    std::string channel = std::string(static_cast<const char*>(msg.data()));
                    msg.rebuild();

                    // receive message body
                    s->sock->recv(&msg);
                    BSONObj message(static_cast<const char*>(msg.data()));
                    message = message.getOwned();
                    msg.rebuild();

                    // receive timestamp
                    s->sock->recv(&msg);
                    unsigned long long timestamp = *((unsigned long long*)(msg.data()));
                    msg.rebuild();

                    // if subscription has filter, continue only if message matches filter
                    if (s->filter && !s->filter->matches(message))
                        continue;

                    // if subscription has projection, apply projection to message
                    if (s->projection)
                        message = s->projection->transform(message);

                    SubscriptionMessage m(subscriptionId, channel, message, timestamp);
                    outbox.push(m);
                }
            }
            catch (zmq::error_t& e) {
                errors.insert(std::make_pair(subscriptionId,
                                             "Error receiving messages from zmq socket."));
            }

            // done receiving from ZMQ socket
            PubSub::checkinSocket(s);
        }

        return outbox;
    }

    void PubSub::unsubscribe(const SubscriptionId& subscriptionId,
                             std::map<SubscriptionId, std::string>& errors,
                             bool force) {
        SimpleMutex::scoped_lock lk(mapMutex);
        SubscriptionMap::iterator it = subscriptions.find(subscriptionId);

        if (it == subscriptions.end()) {
            errors.insert(std::make_pair(subscriptionId, "Subscription not found."));
            return;
        }

        // if force unsubscribe not specified, set flag to unsubscribe when poll checks
        // active polls check frequently if unsubscribe has been noted
        shared_ptr<SubscriptionInfo> s = it->second;
        if (!force && s->inUse) {
            s->shouldUnsub = 1;
        }
        else {
            try {
                s->sock->close();
            }
            catch (zmq::error_t& e) {
                errors.insert(std::make_pair(subscriptionId, "Error closing zmq socket."));
            }
            subscriptions.erase(it);
        }
    }

}  // namespace mongo
