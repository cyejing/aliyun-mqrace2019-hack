package io.openmessaging;

/**
 * @author Born
 */
public class ByteUtil {

    public static byte[] toIntBytes(int x) {
        return new byte[]{long1(x), long0(x)};
    }

    public static int getInt(byte b1,byte b0) {
        return makeInt(b1, b0);
    }


    static private int makeInt(byte b1, byte b0) {
        return (((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    static private int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    private static byte long7(long x) { return (byte)(x >> 56); }
    private static byte long6(long x) { return (byte)(x >> 48); }
    private static byte long5(long x) { return (byte)(x >> 40); }
    private static byte long4(long x) { return (byte)(x >> 32); }
    private static byte long3(long x) { return (byte)(x >> 24); }
    private static byte long2(long x) { return (byte)(x >> 16); }
    private static byte long1(long x) { return (byte)(x >>  8); }
    private static byte long0(long x) { return (byte)(x      ); }



}
