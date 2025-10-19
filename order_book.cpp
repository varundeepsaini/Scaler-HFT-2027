#include <cstdint>
#include <vector>
#include <unordered_map>
#include <map>
#include <memory>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <limits>
#include <cassert>
#include <random>
#include <chrono>

using Price = double;

struct PriceLevel {
    double price;
    uint64_t total_quantity;
};

constexpr size_t MEMORY_POOL_BLOCK_SIZE = 1024;
constexpr size_t MAX_ORDER_QUANTITY = 1000000;
constexpr double MIN_PRICE = 0.01;
constexpr double MAX_PRICE = 1000000.0;


template<typename T>
class SimpleMemoryPool {
private:
    static constexpr size_t BLOCK_SIZE = MEMORY_POOL_BLOCK_SIZE;
    std::vector<std::unique_ptr<T[]>> blocks_;
    size_t current_block_index_{0};
    size_t current_position_{0};
    T* free_list_head_{nullptr};

public:
    SimpleMemoryPool() {
        allocate_new_block();
    }

    T* allocate() {
        T* ptr = pop_free_list();
        if (ptr) {
            return ptr;
        }

        if (current_position_ >= BLOCK_SIZE) {
            allocate_new_block();
        }

        return &blocks_[current_block_index_][current_position_++];
    }

    void deallocate(T* ptr) {
        if (ptr) {
            push_free_list(ptr);
        }
    }

private:
    void allocate_new_block() {
        try {
            blocks_.emplace_back(std::make_unique<T[]>(BLOCK_SIZE));
            current_block_index_ = blocks_.size() - 1;
            current_position_ = 0;
        } catch (const std::bad_alloc& e) {
            std::cerr << "Error: Failed to allocate memory block: " << e.what() << "\n";
            throw;
        }
    }

    T* pop_free_list() {
        if (free_list_head_) {
            T* head = free_list_head_;
            free_list_head_ = *reinterpret_cast<T**>(head);
            return head;
        }
        return nullptr;
    }

    void push_free_list(T* ptr) {
        *reinterpret_cast<T**>(ptr) = free_list_head_;
        free_list_head_ = ptr;
    }
};

struct Order {
    uint64_t order_id;
    bool is_buy;
    double price;
    uint64_t quantity;
    uint64_t timestamp_ns;
    Order* next{nullptr};
    Order* prev{nullptr};
    bool is_active{true};
    
    Order() = default;
    Order(uint64_t id, bool buy, double p, uint64_t qty, uint64_t ts)
        : order_id(id), is_buy(buy), price(p), quantity(qty), timestamp_ns(ts) {}
    
    Order(const Order& other) 
        : order_id(other.order_id), is_buy(other.is_buy),
          price(other.price), quantity(other.quantity),
          timestamp_ns(other.timestamp_ns), next(nullptr), prev(nullptr),
          is_active(other.is_active) {}
    
    Order& operator=(const Order& other) {
        if (this != &other) {
            order_id = other.order_id;
            is_buy = other.is_buy;
            price = other.price;
            quantity = other.quantity;
            timestamp_ns = other.timestamp_ns;
            next = nullptr;
            prev = nullptr;
            is_active = other.is_active;
        }
        return *this;
    }
};

struct InternalPriceLevel {
    double price;
    uint64_t total_quantity{0};
    Order* first_order{nullptr};
    Order* last_order{nullptr};
    size_t order_count{0};
    bool is_active{true};

    InternalPriceLevel() : price(0.0) {}
    InternalPriceLevel(double p) : price(p) {}
    
    InternalPriceLevel(const InternalPriceLevel& other)
        : price(other.price), total_quantity(other.total_quantity),
          first_order(nullptr), last_order(nullptr), order_count(other.order_count),
          is_active(other.is_active) {}
    
    InternalPriceLevel& operator=(const InternalPriceLevel& other) {
        if (this != &other) {
            price = other.price;
            total_quantity = other.total_quantity;
            first_order = nullptr;
            last_order = nullptr;
            order_count = other.order_count;
            is_active = other.is_active;
        }
        return *this;
    }

    void add_order(Order* order) {
        if (first_order == nullptr) {
            first_order = order;
            last_order = order;
        } else {
            order->prev = last_order;
            if (last_order) {
                last_order->next = order;
            }
            last_order = order;
        }
        
        total_quantity += order->quantity;
        order_count++;
    }

    void remove_order(Order* order) {
        if (!order->is_active) {
            return;
        }
        
        order->is_active = false;
        
        Order* prev = order->prev;
        Order* next = order->next;
        
        if (prev) {
            prev->next = next;
        } else {
            first_order = next;
        }
        
        if (next) {
            next->prev = prev;
        } else {
            last_order = prev;
        }
        
        total_quantity -= order->quantity;
        order_count--;
    }

    bool is_empty() const {
        return order_count == 0;
    }
};

class OrderBook {
private:
    std::map<Price, InternalPriceLevel*, std::greater<Price>> bids_;
    std::map<Price, InternalPriceLevel*> asks_;
    std::unordered_map<uint64_t, Order*> order_lookup_;
    SimpleMemoryPool<Order> order_pool_;
    SimpleMemoryPool<InternalPriceLevel> level_pool_;
    
    bool matching_in_progress_{false};
    uint64_t version_{0};

public:
    ~OrderBook() {
        for (auto& [id, order] : order_lookup_) {
            if (order) {
                order_pool_.deallocate(order);
            }
        }
        order_lookup_.clear();
        
        for (auto& [price, level] : bids_) {
            if (level) {
                level_pool_.deallocate(level);
            }
        }
        bids_.clear();
        
        for (auto& [price, level] : asks_) {
            if (level) {
                level_pool_.deallocate(level);
            }
        }
        asks_.clear();
    }

    bool add_order(const Order& o) {
        if (o.order_id == 0) {
            std::cerr << "Error: Invalid order ID (0)\n";
            return false;
        }
        
        if (o.price < MIN_PRICE || o.price > MAX_PRICE || std::isnan(o.price) || std::isinf(o.price)) {
            std::cerr << "Error: Invalid price: " << o.price << " (must be between " << MIN_PRICE << " and " << MAX_PRICE << ")\n";
            return false;
        }
        
        if (o.quantity == 0 || o.quantity > MAX_ORDER_QUANTITY) {
            std::cerr << "Error: Invalid quantity: " << o.quantity << " (must be between 1 and " << MAX_ORDER_QUANTITY << ")\n";
            return false;
        }
        
        if (order_lookup_.find(o.order_id) != order_lookup_.end()) {
            std::cerr << "Error: Duplicate order ID: " << o.order_id << "\n";
            return false;
        }
        
        Order* new_order = order_pool_.allocate();
        if (!new_order) {
            std::cerr << "Error: Failed to allocate memory for order\n";
            return false;
        }
        
        *new_order = o;
        new_order->next = nullptr;
        new_order->prev = nullptr;
        new_order->is_active = true;
        
        order_lookup_[o.order_id] = new_order;
        
        InternalPriceLevel* level = get_or_create_level(o.price, o.is_buy);
        if (!level) {
            order_lookup_.erase(o.order_id);
            order_pool_.deallocate(new_order);
            return false;
        }
        
        level->add_order(new_order);
        version_++;
        
        match_orders();
        return true;
    }

    bool cancel_order(uint64_t id) {
        if (id == 0) {
            std::cerr << "Error: Invalid order ID (0)\n";
            return false;
        }
        
        auto it = order_lookup_.find(id);
        if (it == order_lookup_.end()) {
            std::cerr << "Error: Order not found: " << id << "\n";
            return false;
        }
        
        Order* order = it->second;
        order_lookup_.erase(it);
        
        if (!order || !order->is_active) {
            if (order) {
                order_pool_.deallocate(order);
            }
            return false;
        }
        
        InternalPriceLevel* level = get_level(order->price, order->is_buy);
        
        if (level) {
            level->remove_order(order);
            
            if (level->is_empty()) {
                remove_price_level(level->price, order->is_buy);
            }
        }
        
        order_pool_.deallocate(order);
        version_++;
        return true;
    }

    bool amend_order(uint64_t order_id, double new_price, uint64_t new_quantity) {
        if (order_id == 0) {
            std::cerr << "Error: Invalid order ID (0)\n";
            return false;
        }
        
        if (new_price < MIN_PRICE || new_price > MAX_PRICE || std::isnan(new_price) || std::isinf(new_price)) {
            std::cerr << "Error: Invalid price: " << new_price << " (must be between " << MIN_PRICE << " and " << MAX_PRICE << ")\n";
            return false;
        }
        
        if (new_quantity == 0 || new_quantity > MAX_ORDER_QUANTITY) {
            std::cerr << "Error: Invalid quantity: " << new_quantity << " (must be between 1 and " << MAX_ORDER_QUANTITY << ")\n";
            return false;
        }
        
        auto it = order_lookup_.find(order_id);
        if (it == order_lookup_.end()) {
            std::cerr << "Error: Order not found: " << order_id << "\n";
            return false;
        }
        
        Order* order = it->second;
        if (!order || !order->is_active) {
            std::cerr << "Error: Order is not active: " << order_id << "\n";
            return false;
        }
        
        if (order->price != new_price) {
            InternalPriceLevel* old_level = get_level(order->price, order->is_buy);
            if (old_level) {
                old_level->remove_order(order);
                if (old_level->is_empty()) {
                    remove_price_level(old_level->price, order->is_buy);
                }
            }
            
            order->price = new_price;
            order->quantity = new_quantity;
            
            InternalPriceLevel* new_level = get_or_create_level(new_price, order->is_buy);
            if (!new_level) {
                std::cerr << "Error: Failed to create price level for " << new_price << "\n";
                return false;
            }
            new_level->add_order(order);
        } else {
            InternalPriceLevel* level = get_level(order->price, order->is_buy);
            if (level) {
                level->total_quantity = level->total_quantity - order->quantity + new_quantity;
                order->quantity = new_quantity;
            } else {
                std::cerr << "Error: Price level not found for order " << order_id << "\n";
                return false;
            }
        }
        
        version_++;
        return true;
    }

    void match_orders() {
        if (matching_in_progress_) {
            return;
        }
        
        matching_in_progress_ = true;
        
        bool matched = true;
        while (matched && !bids_.empty() && !asks_.empty()) {
            matched = false;
            
            auto best_bid = bids_.begin();
            auto best_ask = asks_.begin();
            
            if (best_bid->first < best_ask->first) {
                break;
            }
            
            InternalPriceLevel* bid_level = best_bid->second;
            InternalPriceLevel* ask_level = best_ask->second;
            
            if (!bid_level->is_active || !ask_level->is_active) {
                break;
            }
            
            Order* bid_order = bid_level->first_order;
            Order* ask_order = ask_level->first_order;
            
            if (!bid_order || !ask_order || !bid_order->is_active || !ask_order->is_active) {
                break;
            }
            
            uint64_t bid_qty = bid_order->quantity;
            uint64_t ask_qty = ask_order->quantity;
            uint64_t match_quantity = std::min(bid_qty, ask_qty);
            
            double match_price = (bid_order->timestamp_ns <= ask_order->timestamp_ns) 
                                ? bid_order->price : ask_order->price;
            
            std::cout << "MATCH: " << match_quantity << " @ " << match_price 
                      << " (Bid: " << bid_order->order_id << ", Ask: " << ask_order->order_id << ")\n";
            
            bid_order->quantity -= match_quantity;
            ask_order->quantity -= match_quantity;
            
            bool bid_removed = remove_filled_order(bid_order, bid_level, best_bid, true);
            bool ask_removed = remove_filled_order(ask_order, ask_level, best_ask, false);
            
            matched = true;
            
            if (bid_removed || ask_removed) {
                continue;
            }
        }
        
        matching_in_progress_ = false;
    }

    void print_book(size_t depth = 10) const {
        std::cout << "\n=== ORDER BOOK ===\n";
        std::cout << "Bids (Buy)          | Asks (Sell)\n";
        std::cout << "Price    | Quantity | Price    | Quantity\n";
        std::cout << "---------|----------|----------|----------\n";
        
        auto bid_it = bids_.begin();
        auto ask_it = asks_.begin();
        
        for (size_t i = 0; i < depth && (bid_it != bids_.end() || ask_it != asks_.end()); ++i) {
            std::cout << std::fixed << std::setprecision(2);
            
            if (bid_it != bids_.end()) {
                double price = bid_it->second->price;
                uint64_t qty = bid_it->second->total_quantity;
                std::cout << std::setw(8) << price << " | " << std::setw(8) << qty;
                ++bid_it;
            } else {
                std::cout << "         |          ";
            }
            
            std::cout << " | ";
            
            if (ask_it != asks_.end()) {
                double price = ask_it->second->price;
                uint64_t qty = ask_it->second->total_quantity;
                std::cout << std::setw(8) << price << " | " << std::setw(8) << qty;
                ++ask_it;
            } else {
                std::cout << "         |          ";
            }
            
            std::cout << "\n";
        }
        
        std::cout << "\nBest Bid: " << get_best_bid() << "\n";
        std::cout << "Best Ask: " << get_best_ask() << "\n";
        std::cout << "Spread: " << get_spread() << "\n";
    }

    
    void get_snapshot(size_t depth, std::vector<PriceLevel>& bids, std::vector<PriceLevel>& asks) const {
        bids.clear();
        asks.clear();
        
        auto bid_it = bids_.begin();
        for (size_t i = 0; i < depth && bid_it != bids_.end(); ++i, ++bid_it) {
            PriceLevel level;
            level.price = bid_it->second->price;
            level.total_quantity = bid_it->second->total_quantity;
            bids.push_back(level);
        }
        
        auto ask_it = asks_.begin();
        for (size_t i = 0; i < depth && ask_it != asks_.end(); ++i, ++ask_it) {
            PriceLevel level;
            level.price = ask_it->second->price;
            level.total_quantity = ask_it->second->total_quantity;
            asks.push_back(level);
        }
    }

    double get_best_bid() const {
        if (bids_.empty()) return 0.0;
        return bids_.begin()->second->price;
    }

    double get_best_ask() const {
        if (asks_.empty()) return std::numeric_limits<double>::max();
        return asks_.begin()->second->price;
    }

    double get_spread() const {
        double best_ask_price = get_best_ask();
        return best_ask_price == std::numeric_limits<double>::max() ? 0.0 : best_ask_price - get_best_bid();
    }

    uint64_t get_version() const {
        return version_;
    }
    
    size_t get_order_count() const {
        return order_lookup_.size();
    }
    
    size_t get_bid_levels() const {
        return bids_.size();
    }
    
    size_t get_ask_levels() const {
        return asks_.size();
    }

private:

    InternalPriceLevel* get_or_create_level(Price price, bool is_buy) {
        if (is_buy) {
            auto it = bids_.find(price);
            if (it != bids_.end()) {
                return it->second;
            }
            
            InternalPriceLevel* level = level_pool_.allocate();
            level->price = price;
            level->total_quantity = 0;
            level->order_count = 0;
            level->is_active = true;
            bids_[price] = level;
            return level;
        } else {
            auto it = asks_.find(price);
            if (it != asks_.end()) {
                return it->second;
            }
            
            InternalPriceLevel* level = level_pool_.allocate();
            level->price = price;
            level->total_quantity = 0;
            level->order_count = 0;
            level->is_active = true;
            asks_[price] = level;
            return level;
        }
    }

    InternalPriceLevel* get_level(Price price, bool is_buy) const {
        if (is_buy) {
            auto it = bids_.find(price);
            return (it != bids_.end()) ? it->second : nullptr;
        } else {
            auto it = asks_.find(price);
            return (it != asks_.end()) ? it->second : nullptr;
        }
    }
    
    bool has_order(uint64_t order_id) const {
        return order_lookup_.find(order_id) != order_lookup_.end();
    }
    
    bool remove_filled_order(Order* order, InternalPriceLevel* level, 
                           const std::map<Price, InternalPriceLevel*>::iterator& map_it, bool is_buy) {
        if (order->quantity == 0) {
            level->remove_order(order);
            
            order_lookup_.erase(order->order_id);
            order_pool_.deallocate(order);
            
            if (level->is_empty()) {
                if (is_buy) {
                    bids_.erase(map_it);
                } else {
                    asks_.erase(map_it);
                }
                level_pool_.deallocate(level);
                return true;
            }
        }
        return false;
    }
    
    void remove_price_level(Price price, bool is_buy) {
        if (is_buy) {
            auto it = bids_.find(price);
            if (it != bids_.end()) {
                level_pool_.deallocate(it->second);
                bids_.erase(it);
            }
        } else {
            auto it = asks_.find(price);
            if (it != asks_.end()) {
                level_pool_.deallocate(it->second);
                asks_.erase(it);
            }
        }
    }
};

void test_order_book() {
    OrderBook book;
    
    book.add_order({1, true, 100.50, 1000, 1234567890});
    book.add_order({2, true, 100.25, 500, 1234567891});
    book.add_order({3, false, 100.75, 750, 1234567892});
    book.add_order({4, false, 100.60, 300, 1234567893});
    
    std::cout << "Initial book:\n";
    book.print_book();
    
    book.add_order({5, true, 100.80, 200, 1234567894});
    
    std::cout << "\nAfter matching:\n";
    book.print_book();
    
    std::vector<PriceLevel> bids, asks;
    book.get_snapshot(3, bids, asks);
    
    std::cout << "\nSnapshot (top 3 levels):\n";
    std::cout << "Bids:\n";
    for (const auto& bid : bids) {
        std::cout << "  " << bid.price << " : " << bid.total_quantity << "\n";
    }
    std::cout << "Asks:\n";
    for (const auto& ask : asks) {
        std::cout << "  " << ask.price << " : " << ask.total_quantity << "\n";
    }
    
    book.cancel_order(2);
    std::cout << "\nAfter canceling order 2:\n";
    book.print_book();
    
    book.add_order({6, true, 100.30, 200, 1234567895});
    book.add_order({7, false, 100.70, 300, 1234567896});
    
    std::cout << "\nAfter adding orders 6 and 7:\n";
    book.print_book();
    
    std::cout << "\n=== Testing amend_order ===\n";
    
    std::cout << "\nAmending order 6 quantity from 200 to 400 (same price):\n";
    bool amend1 = book.amend_order(6, 100.30, 400);
    std::cout << "Result: " << (amend1 ? "Success" : "Failed") << "\n";
    book.print_book();
    
    std::cout << "\nAmending order 7 price from 100.70 to 100.80 (price change):\n";
    bool amend2 = book.amend_order(7, 100.80, 300);
    std::cout << "Result: " << (amend2 ? "Success" : "Failed") << "\n";
    book.print_book();
    
    std::cout << "\nTrying to amend non-existent order 999:\n";
    bool amend3 = book.amend_order(999, 100.0, 100);
    std::cout << "Result: " << (amend3 ? "Success" : "Failed") << "\n";
}

void stress_test() {
    OrderBook book;
    const int total_orders = 10000;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> price_dist(100.0, 110.0);
    std::uniform_int_distribution<> qty_dist(1, 1000);
    std::uniform_int_distribution<> bool_dist(0, 1);
    
    for (int i = 0; i < total_orders; ++i) {
        bool is_buy = bool_dist(gen);
        double price = price_dist(gen);
        uint64_t qty = qty_dist(gen);
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();
        
        book.add_order({static_cast<uint64_t>(i), is_buy, price, qty, timestamp});
        
        if (i % 100 == 0) {
            book.cancel_order(i - 50);
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "\nStress test completed:\n";
    std::cout << "Total orders: " << total_orders << "\n";
    std::cout << "Time taken: " << duration.count() << " ms\n";
    std::cout << "Orders per second: " << (total_orders * 1000) / duration.count() << "\n";
    
    book.print_book(5);
}

int main() {
    std::cout << "=== Order Book Test ===\n";
    test_order_book();
    
    std::cout << "\n=== Stress Test ===\n";
    stress_test();
    
    return 0;
}
