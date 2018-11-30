package com.jouryu.netty;

import com.jouryu.constants.NettyContants;
import com.jouryu.utils.AesEncryptUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Created by tomorrow on 18/11/9.
 *
 * websocket client, 把计算结果发送给socket server
 */

@Component
public class SocketClient {

    @Value("${socketServer.host}")
    private String socketServerHost;

    @Value("${socketServer.port}")
    private int socketServerPort;

    private static final Logger logger = LoggerFactory.getLogger(SocketClient.class);

    public static Channel channel;

    public void start() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            final WebSocketClientHandler handler =
                    new WebSocketClientHandler(
                            this,
                            WebSocketClientHandshakerFactory.newHandshaker(
                                    new URI("ws://" + socketServerHost + ":" + socketServerPort + NettyContants.SOCKET_SERVER_PATH),
                                    WebSocketVersion.V13, null, false, new DefaultHttpHeaders()
                            )
                    );

            Bootstrap b = new Bootstrap();
            b.option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(8192),
                                    handler);
                        }
                    });

            channel = b.connect(socketServerHost, socketServerPort)
                    .addListener(new ConnectionListener(this))  // 连接失败后要不断重连
                    .sync()
                    .channel();
            handler.handshakeFuture().sync();
            channel.closeFuture().sync();
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
//          因为需要不断重连, 所以这里不能释放
//          group.shutdownGracefully();
        }

    }

    private class ConnectionListener implements ChannelFutureListener {
        private SocketClient client;

        ConnectionListener(SocketClient client) {
            this.client = client;
        }

        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
            if (!channelFuture.isSuccess()) {
                final EventLoop loop = channelFuture.channel().eventLoop();
                loop.schedule(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("WebSocket Client Reconnect");
                        client.start();
                    }
                }, 1L, TimeUnit.SECONDS);
            }
        }
    }

    private class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private SocketClient socketClient;

        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;


        public WebSocketClientHandler(SocketClient client, WebSocketClientHandshaker handshaker) {
            this.socketClient = client;
            this.handshaker = handshaker;
        }

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("WebSocket Client disconnected!");

            // 中断后需要不断重连
            final EventLoop eventLoop = ctx.channel().eventLoop();
            eventLoop.schedule(new Runnable() {
                @Override
                public void run() {
                    System.out.println("WebSocket Client Reconnect");
                    socketClient.start();
                }
            }, 1L, TimeUnit.SECONDS);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                System.out.println("WebSocket Client connected!");
                handshakeFuture.setSuccess();

                // 初次建立连接后, 简单做个效验,
                String token = AesEncryptUtils.encrypt(NettyContants.SOCKET_AUTH_TOKEN);
                TextWebSocketFrame frame = new TextWebSocketFrame("token:" + token);
                SocketClient.channel.writeAndFlush(frame);
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException(
                        "Unexpected FullHttpResponse (content=" + response.content().toString(CharsetUtil.UTF_8) + ")");
            }

            WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof TextWebSocketFrame) {
                TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                System.out.println("WebSocket Client received message: " + textFrame.text());
            } else if (frame instanceof PongWebSocketFrame) {
                System.out.println("WebSocket Client received pong");
            } else if (frame instanceof CloseWebSocketFrame) {
                System.out.println("WebSocket Client received closing");
                ch.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            if (!handshakeFuture.isDone()) {
                handshakeFuture.setFailure(cause);
            }
            ctx.close();
        }
    }
}
