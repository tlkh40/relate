package me.lusory.relate.query.criteria;

public interface CriteriaVisitor<T> {

    T visit(Criteria.And and);

    T visit(Criteria.Or or);

    T visit(Criteria.PropertyOperation op);

    abstract class SearchVisitor implements CriteriaVisitor<Boolean> {
        @Override
        public Boolean visit(Criteria.And and) {
            return and.getLeft().accept(this)
                    || and.getRight().accept(this);
        }

        @Override
        public Boolean visit(Criteria.Or or) {
            return or.getLeft().accept(this)
                    || or.getRight().accept(this);
        }
    }
}
