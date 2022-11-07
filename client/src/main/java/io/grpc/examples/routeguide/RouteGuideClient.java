package io.grpc.examples.routeguide;

import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RouteGuideClient {

    private static final Logger logger = Logger.getLogger(RouteGuideClient.class.getName());
    private final RouteGuideGrpc.RouteGuideBlockingStub blockingStub;
    private final RouteGuideGrpc.RouteGuideStub asyncStub;

    private Random random = new Random();

    public RouteGuideClient(Channel channel) {
        blockingStub = RouteGuideGrpc.newBlockingStub(channel);
        asyncStub = RouteGuideGrpc.newStub(channel);
    }

    private Feature getFeature(int lat, int lon) {
        info("*** GetFeature: lat={0} lon={1}", lat, lon);
        Point request = Point.newBuilder().setLatitude(lat).setLatitude(lon).build();
        Feature feature = null;
        try {
            feature = blockingStub.getFeature(request);
        } catch (StatusRuntimeException e) {
            warning("RPC failed: {0}", e.getStatus());
        }
        return feature;
    }

    private Iterator<Feature> listFeatures(int lowLat, int lowLon, int hiLat, int hiLon) {
        info("*** ListFeatures: lowLat={0} lowLon={1} hiLat={2} hiLon={3}", lowLat, lowLon, hiLat, hiLon);

        Rectangle request = Rectangle.newBuilder()
                .setLo(Point.newBuilder().setLatitude(lowLat).setLongitude(lowLon).build())
                .setHi(Point.newBuilder().setLatitude(hiLat).setLongitude(hiLon).build())
                .build();
        Iterator<Feature> features = null;
        try {
            features = blockingStub.listFeatures(request);
        } catch (StatusRuntimeException e) {
            warning("RPC failed: {0}", e.getStatus());
        }
        return features;
    }

    private void recordRoute(List<Feature> features, int numPoints) throws InterruptedException {
        info("*** RecordRoute");
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<RouteSummary> responseObserver = new StreamObserver<RouteSummary>() {
            @Override
            public void onNext(RouteSummary summary) {
                info("Finished trip with {0} points. Passed {1} features. Travelled {2} meters. It took {3} seconds.",
                        summary.getPointCount(),
                        summary.getFeatureCount(),
                        summary.getDistance(),
                        summary.getElapsedTime());
            }

            @Override
            public void onError(Throwable t) {
                warning("RecordRoute Failed: {0}", Status.fromThrowable(t));
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                info("Finished RecordRoute");
                finishLatch.countDown();
            }
        };

        StreamObserver<Point> requestObserver = asyncStub.recordRoute(responseObserver);
        try {
            for (int i = 0; i < numPoints; i++) {
                int index = random.nextInt(features.size());
                Point point = features.get(index).getLocation();
                info("Visiting point {0}, {1}", RouteGuideUtil.getLatitude(point), RouteGuideUtil.getLongitude(point));
                requestObserver.onNext(point);
                Thread.sleep(random.nextInt(1000) + 500);
                if (finishLatch.getCount() == 0) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();

        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            warning("recordRoute can not finish within 1 minutes");
        }
    }

    private CountDownLatch routeChat() {
        info("*** RouteChat");
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<RouteNote> responseObserver = new StreamObserver<RouteNote>() {
            @Override
            public void onNext(RouteNote note) {
                info("Got message \"{0}\" at {1}, {2}",
                        note.getMessage(),
                        note.getLocation().getLatitude(),
                        note.getLocation().getLongitude());
            }

            @Override
            public void onError(Throwable t) {
                warning("RouteChat Failed: {0}", Status.fromThrowable(t));
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                info("Finished RouteChat");
                finishLatch.countDown();
            }
        };

        StreamObserver<RouteNote> requestObserver = asyncStub.routeChat(responseObserver);

        RouteNote[] requests = {
                newNote("First message", 0, 0),
                newNote("Second message", 0, 10_000_000),
                newNote("Third message", 10_000_000, 0),
                newNote("Fourth message", 10_000_000, 10_000_000),
        };

        try {
            for (RouteNote request : requests) {
                info("Sending message \"{0}\" at {1}, {2}",
                        request.getMessage(),
                        request.getLocation().getLatitude(),
                        request.getLocation().getLongitude());
                requestObserver.onNext(request);
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();

        return finishLatch;
    }

    private RouteNote newNote(String message, int lat, int lon) {
        return RouteNote.newBuilder()
                .setMessage(message)
                .setLocation(Point.newBuilder().setLatitude(lat).setLongitude(lon).build())
                .build();
    }

    private void info(String msg, Object... params) {
        logger.log(Level.INFO, msg, params);
    }

    private void warning(String msg, Object... params) {
        logger.log(Level.WARNING, msg, params);
    }

    public static void main(String[] args) throws InterruptedException {
        String target = "localhost:8980";
        List<Feature> features;
        try {
            features = RouteGuideUtil.parseFeatures(RouteGuideUtil.getDefaultFeaturesFile());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        try {
            RouteGuideClient client = new RouteGuideClient(channel);
            CountDownLatch finishLatch = client.routeChat();
            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                client.warning("routeChat can not finish within 1 minutes");
            }
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }


}
