package proxy

import (
	"context"
	"net"
	"strconv"
	"time"

	grpctrace "github.com/DataDog/dd-trace-go/contrib/google.golang.org/grpc/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/bibendi/gruf-relay/internal/worker"
)

var _ = Describe("injectSpan", func() {
	It("replaces the caller's trace headers with the relay span's and keeps other metadata", func() {
		mt := mocktracer.Start()
		defer mt.Stop()

		span := tracer.StartSpan("grpc.server")
		defer span.Finish()

		md := metadata.Pairs(
			"x-datadog-trace-id", "111",
			"x-datadog-parent-id", "222",
			"traceparent", "00-0000000000000000000000000000006f-00000000000000de-01",
			"authorization", "token",
		)

		injectSpan(span, md)

		Expect(md.Get("x-datadog-trace-id")).To(HaveLen(1))
		Expect(md.Get("x-datadog-parent-id")).To(Equal([]string{strconv.FormatUint(span.Context().SpanID(), 10)}))
		Expect(md.Get("traceparent")).NotTo(ContainElement("00-0000000000000000000000000000006f-00000000000000de-01"))
		Expect(md.Get("authorization")).To(Equal([]string{"token"}))
	})
})

var _ = Describe("HandleRequest with tracing", func() {
	It("parents the worker request on a relay span that continues the caller's trace", func() {
		mt := mocktracer.Start()
		defer mt.Stop()

		ctrl := gomock.NewController(GinkgoT())
		defer ctrl.Finish()

		listen := func(srv *grpc.Server) *grpc.ClientConn {
			lis := bufconn.Listen(1024 * 1024)
			go func() { _ = srv.Serve(lis) }()
			DeferCleanup(srv.Stop)
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(conn.Close)
			return conn
		}

		workerMD := make(chan metadata.MD, 1)
		workerConn := listen(grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(stream.Context())
			workerMD <- md
			if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
				return err
			}
			return stream.SendMsg(&emptypb.Empty{})
		})))

		pulled := NewMockPulledClientConn(ctrl)
		pulled.EXPECT().Conn().Return(workerConn).AnyTimes()
		pulled.EXPECT().Return().AnyTimes()
		w := worker.NewMockWorker(ctrl)
		w.EXPECT().FetchClientConn(gomock.Any()).Return(pulled, nil)
		w.EXPECT().String().Return("worker-1").AnyTimes()
		balancer := NewMockBalancer(ctrl)
		balancer.EXPECT().Next().Return(w)

		relayConn := listen(grpc.NewServer(
			grpc.UnknownServiceHandler(NewProxy(balancer, 2*time.Second).HandleRequest),
			grpc.StreamInterceptor(grpctrace.StreamServerInterceptor(grpctrace.WithStreamMessages(false))),
		))

		callerCtx := metadata.AppendToOutgoingContext(context.Background(),
			"x-datadog-trace-id", "111", "x-datadog-parent-id", "222", "x-datadog-sampling-priority", "1")
		Expect(relayConn.Invoke(callerCtx, "/test.Service/Method", &emptypb.Empty{}, &emptypb.Empty{})).To(Succeed())

		md := <-workerMD
		Eventually(mt.FinishedSpans).Should(HaveLen(1))
		span := mt.FinishedSpans()[0]

		Expect(span.Tag("resource.name")).To(Equal("/test.Service/Method"))
		Expect(span.Tag("gruf_relay.worker")).To(Equal("worker-1"))
		Expect(span.ParentID()).To(Equal(uint64(222)))
		Expect(md.Get("x-datadog-trace-id")).To(Equal([]string{"111"}))
		Expect(md.Get("x-datadog-parent-id")).To(Equal([]string{strconv.FormatUint(span.SpanID(), 10)}))
	})
})
