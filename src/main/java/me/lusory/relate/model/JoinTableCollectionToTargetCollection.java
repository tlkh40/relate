package me.lusory.relate.model;

import me.lusory.relate.enhance.Enhancer;
import org.springframework.lang.NonNull;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings({"java:S3011"})
public class JoinTableCollectionToTargetCollection<J, T> implements Set<T> {

    private final Object sourceInstance;
    private final Collection<J> sourceCollection;
    private final Class<?> joinClass;
    private final Field sourceField;
    private final Field targetField;

    public JoinTableCollectionToTargetCollection(
            Object sourceInstance,
            Collection<J> sourceCollection,
            String joinClassName,
            int sourceAttributeLinkNumber) {
        this.sourceInstance = sourceInstance;
        this.sourceCollection = sourceCollection;
        try {
            joinClass = getClass().getClassLoader().loadClass(joinClassName);
            sourceField =
                    joinClass.getDeclaredField(
                            Enhancer.JOIN_TABLE_ATTRIBUTE_PREFIX + sourceAttributeLinkNumber);
            sourceField.setAccessible(true);
            targetField =
                    joinClass.getDeclaredField(
                            Enhancer.JOIN_TABLE_ATTRIBUTE_PREFIX
                                    + (sourceAttributeLinkNumber == 1 ? 2 : 1));
            targetField.setAccessible(true);
        } catch (Exception e) {
            throw new ModelAccessException(
                    "Error initializing JoinTableCollectionToTargetCollection on " + joinClassName,
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    private T getTarget(J join) {
        try {
            return (T) targetField.get(join);
        } catch (Exception e) {
            throw new ModelAccessException(
                    "Error getting field "
                            + targetField.getName()
                            + " on join table class "
                            + joinClass.getName(),
                    e);
        }
    }

    @Override
    public int size() {
        return sourceCollection.size();
    }

    @Override
    public boolean isEmpty() {
        return sourceCollection.isEmpty();
    }

    @Override
    public void clear() {
        sourceCollection.clear();
    }

    @Override
    public boolean contains(Object o) {
        for (J jt : sourceCollection) {
            if (Objects.equals(o, getTarget(jt))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<J> it = sourceCollection.iterator();
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return getTarget(it.next());
            }
        };
    }

    @Override
    public Object[] toArray() {
        return toArray(new Object[sourceCollection.size()]);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R[] toArray(R[] a) {
        if (a.length < sourceCollection.size()) {
            a =
                    (R[])
                            java.lang.reflect.Array.newInstance(
                                    a.getClass().getComponentType(), sourceCollection.size());
        }
        int i = 0;
        for (T e : this) {
            a[i++] = (R) e;
        }
        return a;
    }

    @Override
    public boolean add(T e) {
        if (contains(e)) {
            return false;
        }
        try {
            @SuppressWarnings("unchecked")
            J join = (J) joinClass.getConstructor().newInstance();
            sourceField.set(join, sourceInstance);
            targetField.set(join, e);
            sourceCollection.add(join);
            return true;
        } catch (Exception err) {
            throw new ModelAccessException(
                    "Error instantiating join entity " + joinClass.getName(), err);
        }
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        boolean result = false;
        for (T e : c) {
            result |= add(e);
        }
        return result;
    }

    @Override
    public boolean remove(Object o) {
        for (Iterator<J> it = sourceCollection.iterator(); it.hasNext(); ) {
            if (Objects.equals(o, getTarget(it.next()))) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean result = false;
        for (Object e : c) {
            result |= remove(e);
        }
        return result;
    }

    @Override
    public boolean retainAll(@NonNull Collection<?> c) {
        boolean result = false;
        for (Iterator<J> it = sourceCollection.iterator(); it.hasNext(); ) {
            if (!c.contains(getTarget(it.next()))) {
                it.remove();
                result = true;
            }
        }
        return result;
    }
}
