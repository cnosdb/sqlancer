package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBWindowFunctionExpression.CnosDBWindowFunction;
import sqlancer.common.ast.FunctionNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CnosDBWindowFunctionExpression extends FunctionNode<CnosDBWindowFunction, CnosDBExpression> implements CnosDBExpression {
    public enum CnosDBWindowFunction {

        ROW_NUMBER(CnosDBDataType.INT),
        RANK(CnosDBDataType.INT),
        DENSE_RANK(CnosDBDataType.INT),
        PERCENT_RANK(CnosDBDataType.DOUBLE),
        CUME_DIST(CnosDBDataType.DOUBLE),
        NTILE(CnosDBDataType.INT) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{CnosDBDataType.INT};
            }
        }, //
        // UINT

        LAG() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType, CnosDBDataType.INT, returnType};
            }
        }, //
        LEAD() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType, CnosDBDataType.INT, returnType};
            }
        }, //
        FIRST_VALUE() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },
        LAST_VALUE() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },
        NTH_VALUE() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType, CnosDBDataType.INT};
            }
        },

        MIN() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },

        MAX() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },

        SUM(CnosDBDataType.INT) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },
        AVG(CnosDBDataType.DOUBLE, CnosDBDataType.INT) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },

        COUNT(CnosDBDataType.INT) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};

            }
        },
        APPROX_MEDIAN(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
            }
        },
//        VAR(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//
//        },
//        VAR_SAMP(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        VAR_POP(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        STDDEV(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        STDDEV_SAMP(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        STDDEV_POP(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        COVAR(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        COVAR_POP(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        COVAR_SAMP(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return new CnosDBDataType[]{Randomly.fromOptions(CnosDBDataType.DOUBLE, CnosDBDataType.INT)};
//            }
//        },
//        CORR(CnosDBDataType.DOUBLE) {
//            @Override
//            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
//                return super.getInputTypes(returnType);
//            }
//        },

        GROUPING(CnosDBDataType.INT) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{CnosDBDataType.getRandomTypeWithoutTimeStamp()};
            }
        },
        APPROX_DISTINCT() {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType};
            }
        },
        APPROX_PERCENTILE_CONT(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE};
            }
        },
        APPROX_PERCENTILE_CONT_WITH_WEIGHT(CnosDBDataType.DOUBLE) {
            @Override
            public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
                return new CnosDBDataType[]{returnType, CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE};
            }
        }
        // TODO array_agg
//        ARRAY_AGG(CnosDBDataType.STRING),
        ;

        CnosDBWindowFunction(CnosDBDataType... supportReturnType) {
            this.supportReturnType = new ArrayList<>(Arrays.asList(supportReturnType));
        }

        private boolean supportReturnType(CnosDBDataType dataType) {
            if (supportReturnType.isEmpty()) {
                return true;
            }
            return supportReturnType.contains(dataType);
        }


        public static CnosDBWindowFunction getRandom(CnosDBDataType dataType) {
            List<CnosDBWindowFunction> options = new ArrayList<>(Arrays.asList(values()));
            if (dataType != CnosDBDataType.INT) {
                options.remove(ROW_NUMBER);
                options.remove(RANK);
                options.remove(DENSE_RANK);
                options.remove(PERCENT_RANK);
                options.remove(NTILE);
                options.remove(CUME_DIST);
            }
            return Randomly.fromList(options);
        }


        public CnosDBDataType[] getInputTypes(CnosDBDataType returnType) {
            return new CnosDBDataType[]{};
        }

        private List<CnosDBDataType> supportReturnType;

    }

    private List<CnosDBExpression> partitionBy = new ArrayList<>();
    private List<CnosDBExpression> orderBy = new ArrayList<>();

    //    private CnosDBExpression filterClause;
    private CnosDBExpression frameSpec;
    //    private CnosDBFrameSpecExclude exclude;
    private CnosDBFrameSpecKind frameSpecKind;


    public static class CnosDBWindowFunctionFrameSpecTerm implements CnosDBExpression {

        public enum CnosDBWindowFunctionFrameSpecTermKind {
            EXPR_PRECEDING("PRECEDING"), CURRENT_ROW("CURRENT ROW"),
            EXPR_FOLLOWING("FOLLOWING");

            String s;

            CnosDBWindowFunctionFrameSpecTermKind(String s) {
                this.s = s;
            }

            public String getString() {
                return s;
            }

        }

        private final CnosDBExpression expression;
        private final CnosDBWindowFunctionFrameSpecTermKind kind;

        public CnosDBWindowFunctionFrameSpecTerm(CnosDBExpression expression,
                                                 CnosDBWindowFunctionFrameSpecTermKind kind) {
            this.expression = expression;
            this.kind = kind;
        }

        public CnosDBWindowFunctionFrameSpecTerm(CnosDBWindowFunctionFrameSpecTermKind kind) {
            this.kind = kind;
            this.expression = null;
        }

        public CnosDBExpression getExpression() {
            return expression;
        }

        public CnosDBWindowFunctionFrameSpecTermKind getKind() {
            return kind;
        }

    }

    public static class CnosDBWindowFunctionFrameSpecBetween implements CnosDBExpression {

        private final CnosDBWindowFunctionFrameSpecTerm left;
        private final CnosDBWindowFunctionFrameSpecTerm right;

        public CnosDBWindowFunctionFrameSpecBetween(CnosDBWindowFunctionFrameSpecTerm left,
                                                    CnosDBWindowFunctionFrameSpecTerm right) {
            this.left = left;
            this.right = right;
        }

        public CnosDBWindowFunctionFrameSpecTerm getLeft() {
            return left;
        }

        public CnosDBWindowFunctionFrameSpecTerm getRight() {
            return right;
        }

    }

    public enum CnosDBFrameSpecKind {
        RANGE, ROWS, GROUPS;

        public static CnosDBFrameSpecKind getRandom() {
            return Randomly.fromOptions(CnosDBFrameSpecKind.values());
        }
    }

    public CnosDBWindowFunctionExpression(CnosDBWindowFunction func, List<CnosDBExpression> args) {
        super(func, args);
    }

    public List<CnosDBExpression> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<CnosDBExpression> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public List<CnosDBExpression> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<CnosDBExpression> orderBy) {
        this.orderBy = orderBy;
    }


    public CnosDBExpression getFrameSpec() {
        return frameSpec;
    }

    public void setFrameSpec(CnosDBExpression frameSpec) {
        this.frameSpec = frameSpec;
    }


    public CnosDBFrameSpecKind getFrameSpecKind() {
        return frameSpecKind;
    }

    public void setFrameSpecKind(CnosDBFrameSpecKind frameSpecKind) {
        this.frameSpecKind = frameSpecKind;
    }


//    public CnosDBFrameSpecExclude getExclude() {
//        return exclude;
//    }
//
//    public void setExclude(CnosDBFrameSpecExclude exclude) {
//        this.exclude = exclude;
//    }

//    public CnosDBExpression getFilterClause() {
//        return filterClause;
//    }


//    public void setFilterClause(CnosDBExpression filterClause) {
//        this.filterClause = filterClause;
//    }
//


    // TODO FrameSpecExclude
//    public enum CnosDBFrameSpecExclude {
//        EXCLUDE_NO_OTHERS("EXCLUDE NO OTHERS"), EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),
//        EXCLUDE_GROUP("EXCLUDE GROUP"), EXCLUDE_TIES("EXCLUDE TIES");
//
//        private final String s;
//
//        CnosDBFrameSpecExclude(String s) {
//            this.s = s;
//        }
//
//        public static CnosDBFrameSpecExclude getRandom() {
//            return Randomly.fromOptions(values());
//        }
//
//        public String getString() {
//            return s;
//        }
//    }

}
