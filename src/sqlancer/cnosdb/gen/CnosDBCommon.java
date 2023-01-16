package sqlancer.cnosdb.gen;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public final class CnosDBCommon {

    private CnosDBCommon() {
    }

    public static boolean appendDataType(CnosDBDataType type, StringBuilder sb) throws AssertionError {
        boolean serial = false;
        switch (type) {
            case BOOLEAN:
                sb.append("BOOLEAN");
                break;
            case INT:
                sb.append("BIGINT");
                break;
            case STRING:
                sb.append("STRING");
                break;
            case DOUBLE:
                sb.append("DOUBLE");
                break;
            case UINT:
                sb.append("BIGINT UNSIGNED");
                break;
            default:
                throw new AssertionError(type);
        }
        return serial;
    }
}
