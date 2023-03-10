//
// Created by harper on 2/5/20.
//

#ifndef CHIDATA_BITMAP_H
#define CHIDATA_BITMAP_H

#include <cstdint>
#include <memory>
#include <vector>

using namespace std;

namespace lqf {

    class Bitset {
    private:
        uint64_t value_;
    public:
        Bitset(uint64_t value);

        uint32_t size();

        bool hasNext();

        uint32_t next();
    };

    class BitmapIterator {
    public:
        virtual ~BitmapIterator() = default;

        virtual void moveTo(uint64_t pos) = 0;

        virtual bool hasNext() = 0;

        virtual uint64_t next() = 0;
    };

    class SimpleBitmapIterator : public BitmapIterator {
    protected:
        uint64_t *content_;
        uint64_t content_size_;
        uint64_t num_bits_;
        uint64_t pointer_;
        uint64_t cached_;
        uint64_t final_mask_;
    public :
        SimpleBitmapIterator(uint64_t *content, uint64_t content_size, uint64_t bit_size);

        virtual ~SimpleBitmapIterator() = default;

        virtual void moveTo(uint64_t pos) override;

        virtual bool hasNext() override;

        virtual uint64_t next() override;
    };

    class SimpleBitmapInvIterator : public BitmapIterator {
    protected:
        uint64_t *content_;
        uint64_t content_size_;
        uint64_t num_bits_;
        uint64_t pointer_;
        uint64_t cached_;
        uint64_t final_mask_;
    public :
        SimpleBitmapInvIterator(uint64_t *content, uint64_t content_size, uint64_t bit_size);

        virtual ~SimpleBitmapInvIterator() = default;

        virtual void moveTo(uint64_t pos) override;

        virtual bool hasNext() override;

        virtual uint64_t next() override;
    };

    class FullBitmapIterator : public BitmapIterator {
    private:
        uint64_t size_;
        uint64_t counter_;
    public :
        FullBitmapIterator(uint64_t size);

        virtual ~FullBitmapIterator() = default;

        virtual void moveTo(uint64_t pos) override;

        virtual bool hasNext() override;

        virtual uint64_t next() override;
    };

    class EmptyBitmapIterator : public BitmapIterator {
    public :
        EmptyBitmapIterator() {};

        virtual ~EmptyBitmapIterator() = default;

        virtual void moveTo(uint64_t pos) override {}

        virtual bool hasNext() override { return false; }

        virtual uint64_t next() override { return -1; }
    };

    class Bitmap : public enable_shared_from_this<Bitmap> {

    public:
        virtual bool check(uint64_t pos) = 0;

        virtual void put(uint64_t pos) = 0;

        virtual void clear() = 0;

        virtual shared_ptr<Bitmap> operator&(Bitmap &x1) = 0;

        virtual shared_ptr<Bitmap> operator|(Bitmap &x1) = 0;

        virtual shared_ptr<Bitmap> operator^(Bitmap &x1) = 0;

        virtual shared_ptr<Bitmap> operator~() = 0;

        virtual uint64_t cardinality() = 0;

        virtual uint64_t size() = 0;

        virtual bool isFull() = 0;

        virtual bool isEmpty() = 0;

        virtual double ratio() = 0;

        // An iterator return the position of all 1s
        virtual unique_ptr<BitmapIterator> iterator() = 0;

        // An iterator return the position of all 0s
        virtual unique_ptr<BitmapIterator> inv_iterator() = 0;

        // Use the given mask to mask the current bitmap
        // Assume the input bitmap has the size equals this->cardinality()
        virtual shared_ptr<Bitmap> mask(Bitmap &) = 0;
    };

    class SimpleBitmap : public Bitmap {
    protected:
        uint64_t *bitmap_;
        uint64_t array_size_;
        uint64_t size_;
        uint64_t first_valid_ = -1;
//        uint64_t cached_cardinality_ = -1;
        // Dirty flag only makes the set 0.5% slower
//        bool dirty_ = true;

        void erase(uint64_t pos);

    public:
        SimpleBitmap(uint64_t size);

        SimpleBitmap(const SimpleBitmap &) = delete;

        SimpleBitmap(SimpleBitmap &&);

        virtual ~SimpleBitmap();

        SimpleBitmap &operator=(const SimpleBitmap &) = delete;

        SimpleBitmap &operator=(SimpleBitmap &&);

        bool check(uint64_t pos) override;

        virtual void put(uint64_t pos) override;

        void clear() override;

        shared_ptr<Bitmap> operator&(Bitmap &x1) override;

        shared_ptr<Bitmap> operator|(Bitmap &x1) override;

        shared_ptr<Bitmap> operator^(Bitmap &x1) override;

        shared_ptr<Bitmap> operator~() override;

        uint64_t cardinality() override;

        uint64_t size() override;

        bool isFull() override;

        bool isEmpty() override;

        double ratio() override;

        std::unique_ptr<BitmapIterator> iterator() override;

        std::unique_ptr<BitmapIterator> inv_iterator() override;

        shared_ptr<Bitmap> mask(Bitmap &) override;

        uint64_t *raw();
    };

    /**
     * Support non-blocking concurrent append
     */
    class ConcurrentBitmap : public SimpleBitmap {
    public:
        ConcurrentBitmap(uint64_t);

        virtual ~ConcurrentBitmap() = default;

        void put(uint64_t) override;
    };

    class RleBitmap : public Bitmap {
    protected:
        uint64_t size_;
        std::vector<pair<uint64_t, uint32_t>> data_;

        // Binary search for a position good for inserting the target
        uint32_t _search(uint64_t target);
    public:
        RleBitmap(uint64_t);

        virtual ~RleBitmap() = default;

        bool check(uint64_t pos) override;

        virtual void put(uint64_t pos) override;

        void clear() override;

        shared_ptr<Bitmap> operator&(Bitmap &x1) override;

        shared_ptr<Bitmap> operator|(Bitmap &x1) override;

        shared_ptr<Bitmap> operator^(Bitmap &x1) override;

        shared_ptr<Bitmap> operator~() override;

        uint64_t cardinality() override;

        uint64_t size() override;

        bool isFull() override;

        bool isEmpty() override;

        double ratio() override;

        std::unique_ptr<BitmapIterator> iterator() override;

        std::unique_ptr<BitmapIterator> inv_iterator() override;

        shared_ptr<Bitmap> mask(Bitmap &) override;
    };

    class FullBitmap : public Bitmap {
    public:
        FullBitmap(uint64_t size);

        virtual ~FullBitmap() = default;

        bool check(uint64_t pos) override;

        void put(uint64_t pos) override;

        void clear() override;

        shared_ptr<Bitmap> operator&(Bitmap &x1) override;

        shared_ptr<Bitmap> operator|(Bitmap &x1) override;

        shared_ptr<Bitmap> operator^(Bitmap &x1) override;

        shared_ptr<Bitmap> operator~() override;

        uint64_t cardinality() override;

        uint64_t size() override;

        bool isFull() override;

        bool isEmpty() override;

        double ratio() override;

        std::unique_ptr<BitmapIterator> iterator() override;

        std::unique_ptr<BitmapIterator> inv_iterator() override;

        shared_ptr<Bitmap> mask(Bitmap &) override;

    private:
        uint64_t size_;
    };
}
#endif //CHIDATA_BITMAP_H
