package net.lecousin.reactive.data.relational.query.criteria;

import net.lecousin.reactive.data.relational.query.criteria.Criteria.And;
import net.lecousin.reactive.data.relational.query.criteria.Criteria.Or;

public interface CriteriaVisitor<T> {

    T visit(Criteria.And and);

    T visit(Criteria.Or or);

    T visit(Criteria.PropertyOperation op);

    abstract class SearchVisitor implements CriteriaVisitor<Boolean> {
        @Override
        public Boolean visit(And and) {
            return and.getLeft().accept(this)
                    || and.getRight().accept(this);
        }

        @Override
        public Boolean visit(Or or) {
            return or.getLeft().accept(this)
                    || or.getRight().accept(this);
        }
    }
}
