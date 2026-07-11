local Class = require("classe").Class
local CompositeDisposable = require("descartavel.composite").CompositeDisposable
local Disposer = require("descartavel.disposer").Disposer

local M = {}

--- @class (exact) ISignalConsumer
--- @field private _notify fun(self: ISignalConsumer): void

--- @class (exact) SignalBatcherClass : Class
--- @field private _current SignalBatcher?
--- @field public  new      fun(self: self): SignalBatcher
local SignalBatcher = Class:new("SignalBatcher")

--- @class (exact) SignalBatcher
--- @field private _depth   number
--- @field private _pending SignalProducer[]
SignalBatcher.prototype = SignalBatcher.prototype

--- @public
--- @return SignalBatcher?
function SignalBatcher:current()
    return self._current
end

--- @private
--- @return void
function SignalBatcher.prototype:__init()
    self._depth = 0
    self._pending = {}
end

--- @public
--- @return void
function SignalBatcher.prototype:activate()
    if self._depth == 0 then SignalBatcher._current = self end

    self._depth = self._depth + 1
end

--- @public
--- @return void
function SignalBatcher.prototype:deactivate()
    self._depth = self._depth - 1

    if self._depth == 0 then
        SignalBatcher._current = nil
        self:_flush()
    end
end

--- @public
--- @param producer SignalProducer
--- @return void
function SignalBatcher.prototype:defer(producer)
    table.insert(self._pending, producer)
end

--- @private
--- @return void
function SignalBatcher.prototype:_flush()
    local current = self._pending
    self._pending = {}

    --- @type table<SignalProducer, true>
    local seen = {}
    for _, producer in ipairs(current) do
        if not seen[producer] then
            seen[producer] = true
            producer:notify()
        end
    end
end

local SIGNAL_BATCHER = SignalBatcher:new()

--- @param block fun(): void
--- @return void
function M.batch(block)
    SIGNAL_BATCHER:activate()
    local ok, e = pcall(block)
    SIGNAL_BATCHER:deactivate()

    if not ok then error(e, 2) end
end

--- @class (exact) SignalProducerClass : Class
--- @field public new fun(self: self): SignalProducer
local SignalProducer = Class:new("SignalProducer")

--- @class (exact) SignalProducer
--- @field private _consumers table<ISignalConsumer, true>
SignalProducer.prototype = SignalProducer.prototype

--- @private
--- @return void
function SignalProducer.prototype:__init()
    self._consumers = {}
end

--- @public
--- @param consumer ISignalConsumer
--- @return void
function SignalProducer.prototype:subscribe(consumer)
    self._consumers[consumer] = true
end

--- @public
--- @param consumer ISignalConsumer
--- @return void
function SignalProducer.prototype:unsubscribe(consumer)
    self._consumers[consumer] = nil
end

--- @public
--- @return void
function SignalProducer.prototype:notify()
    for consumer in pairs(self._consumers) do
        pcall(consumer._notify, consumer)
    end
end

--- @public
--- @return void
function SignalProducer.prototype:batch_notify()
    local batcher = SignalBatcher:current()
    if batcher then
        batcher:defer(self)
    else
        self:notify()
    end
end

--- @class (exact) SignalWatcherClass : Class
--- @field private _current SignalWatcher?
--- @field public  new      fun(self: self): SignalWatcher
local SignalWatcher = Class:new("SignalWatcher")

--- @class (exact) SignalWatcher
--- @field private _parent    SignalWatcher?
--- @field private _producers table<SignalProducer, true>
SignalWatcher.prototype = SignalWatcher.prototype

--- @public
--- @return SignalWatcher?
function SignalWatcher:current()
    return self._current
end

--- @private
--- @return void
function SignalWatcher.prototype:__init() end

--- @public
--- @return void
function SignalWatcher.prototype:activate()
    assert(self._parent == nil, "Watcher is already activated")

    self._parent = SignalWatcher._current
    self._producers = {}
    SignalWatcher._current = self
end

--- @public
--- @return table<SignalProducer, true>
function SignalWatcher.prototype:deactivate()
    assert(SignalWatcher._current == self, "Watcher is not activated")

    SignalWatcher._current = self._parent
    self._parent = nil

    local producers = self._producers
    self._producers = nil

    return producers
end

--- @public
--- @param producer SignalProducer
--- @return void
function SignalWatcher.prototype:watch(producer)
    self._producers[producer] = true
end

--- @class (exact) BlindSignalWatcherClass : Class
--- @field public new fun(self: self): BlindSignalWatcher
local BlindSignalWatcher = Class:extend(SignalWatcher, "BlindSignalWatcher")

--- @class (exact) BlindSignalWatcher : SignalWatcher
BlindSignalWatcher.prototype = BlindSignalWatcher.prototype

--- @see SignalWatcher.watch
function BlindSignalWatcher.prototype:watch() end

SignalWatcher.blind = BlindSignalWatcher:new()

--- @generic T, A
--- @param block fun(...: A...): T
--- @param ... A...
--- @return T
function M.untracked(block, ...)
    SignalWatcher.blind:activate()
    local ok, result = pcall(block, ...)
    _ = SignalWatcher.blind:deactivate()

    if not ok then error(result, 2) end

    return result
end

--- @class (exact) ISignal<T>
--- @field get fun(self: ISignal<T>): T

--- @class (exact) IWritableSignal<T> : ISignal<T>
--- @field set fun(self: IWritableSignal<T>, new_value: T): void

--- @class (exact) SignalClass : Class
--- @field public new fun<T>(self: self, initial_value: T): Signal<T>
local Signal = Class:new("Signal")

--- @class (exact) Signal<T> : IWritableSignal<T>
--- @field private _producer SignalProducer
--- @field private _value    T
Signal.prototype = Signal.prototype

--- @generic T
--- @private
--- @param initial_value T
--- @return void
function Signal.prototype:__init(initial_value)
    self._producer = SignalProducer:new()
    self._value = initial_value
end

--- @see ISignal.get
function Signal.prototype:get()
    local watcher = SignalWatcher:current()
    if watcher then watcher:watch(self._producer) end

    return self._value
end

--- @see IWritableSignal.set
function Signal.prototype:set(new_value)
    local has_changed = self._value ~= new_value

    self._value = new_value
    if has_changed then self._producer:batch_notify() end
end

--- @generic T
--- @param initial_value T
--- @return IWritableSignal<T>
function M.signal(initial_value)
    return Signal:new(initial_value)
end

--- @class (exact) ComputedSignalClass : Class
--- @field public new fun<T>(self: self, compute: (fun(): T)): ComputedSignal<T>
local ComputedSignal = Class:new("ComputedSignal")

--- @class (exact) ComputedSignal<T> : ISignal<T>, ISignalConsumer
--- @field private _own_watcher  SignalWatcher
--- @field private _producers    table<SignalProducer, true>
--- @field private _own_producer SignalProducer
--- @field private _compute      fun(): T
--- @field private _has_value    boolean
--- @field private _value?       T
--- @field private _error?       unknown
ComputedSignal.prototype = ComputedSignal.prototype

--- @generic T
--- @private
--- @param compute fun(): T
--- @return void
function ComputedSignal.prototype:__init(compute)
    self._own_watcher = SignalWatcher:new()
    self._producers = {}
    self._own_producer = SignalProducer:new()

    self._compute = compute
    self._has_value = false
    self._value = nil
    self._error = nil

    self:_notify()
end

--- @see ISignal.get
function ComputedSignal.prototype:get()
    local watcher = SignalWatcher:current()
    if watcher then watcher:watch(self._own_producer) end

    if self._has_value then return self._value end

    error(self._error, 2)
end

--- @see ISignalConsumer._notify
--- @private
function ComputedSignal.prototype:_notify()
    self._own_watcher:activate()
    local ok, value = pcall(self._compute)
    local producers = self._own_watcher:deactivate()

    local has_changed
    if ok then
        has_changed = not self._has_value or self._value ~= value
        self._has_value = true
        self._value = value
        self._error = nil
    else
        -- NOTE: Errors are never the same.
        has_changed = true
        self._has_value = false
        self._value = nil
        self._error = value
    end

    for old_producer in pairs(self._producers) do
        if not producers[old_producer] then old_producer:unsubscribe(self) end
    end

    for new_producer in pairs(producers) do
        if not self._producers[new_producer] then new_producer:subscribe(self) end
    end

    self._producers = producers

    if has_changed then self._own_producer:notify() end
end

--- @generic T
--- @param compute fun(): T
--- @return ISignal<T>
function M.computed(compute)
    return ComputedSignal:new(compute)
end

--- @class (exact) IEffectScope
--- @field disposables IDisposer

--- @alias EffectSetup fun(scope: IEffectScope): void

--- @class (exact) EffectClass : Class
--- @field public new fun(self: self, setup: EffectSetup): Effect
local Effect = Class:new("Effect")

--- @class (exact) Effect : IDisposable, ISignalConsumer
--- @field private _watcher      SignalWatcher
--- @field private _producers    table<SignalProducer, true>
--- @field private _setup        EffectSetup
--- @field private _disposables? CompositeDisposable
Effect.prototype = Effect.prototype

--- @private
--- @param setup EffectSetup
--- @return void
function Effect.prototype:__init(setup)
    self._watcher = SignalWatcher:new()
    self._producers = {}
    self._setup = setup

    self:_notify()
end

--- @see IDisposable.is_disposed
function Effect.prototype:is_disposed()
    return self._watcher == nil
end

--- @see IDisposable.dispose
function Effect.prototype:dispose()
    if self:is_disposed() then return end

    self._watcher = nil
    self._setup = nil
    self:_teardown()

    for producer in pairs(self._producers) do
        producer:unsubscribe(self)
    end

    self._producers = nil
end

--- @see ISignalConsumer._notify
--- @private
function Effect.prototype:_notify()
    if self:is_disposed() then return end

    self:_teardown()

    self._disposables = CompositeDisposable:new()
    local disposer = Disposer:new(self._disposables)

    self._watcher:activate()
    --- @type IEffectScope
    local scope = { disposables = disposer }
    local ok = pcall(self._setup, scope)
    local producers = self._watcher:deactivate()

    if not ok then self:_teardown() end

    for old_producer in pairs(self._producers) do
        if not producers[old_producer] then old_producer:unsubscribe(self) end
    end

    for new_producer in pairs(producers) do
        if not self._producers[new_producer] then new_producer:subscribe(self) end
    end

    self._producers = producers
end

--- @private
--- @return void
function Effect.prototype:_teardown()
    local disposables = self._disposables
    self._disposables = nil

    if disposables then pcall(disposables.dispose, disposables) end
end

--- @param setup EffectSetup
--- @return Effect
function M.effect(setup)
    return Effect:new(setup)
end

return M
