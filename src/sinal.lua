local Class = require("classe").Class
local CompositeDisposable = require("descartavel.composite").CompositeDisposable
local Disposer = require("descartavel.disposer").Disposer

local M = {}

--- @class (exact) ISignalConsumer
--- @field private _notify fun(self: ISignalConsumer): void

--- @class (exact) SignalBatcherClass: Class
--- @field public new       fun(self: self): SignalBatcher
--- @field private _current SignalBatcher?
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
    if self._depth == 0 then
        SignalBatcher._current = self
    end

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
            producer:notify()
            seen[producer] = true
        end
    end
end

local SIGNAL_BATCHER = SignalBatcher:new()

--- @param block fun(): void
--- @return void
function M.batch(block)
    SIGNAL_BATCHER:activate()
    -- TODO: Error handling.
    block()
    SIGNAL_BATCHER:deactivate()
end

--- @class (exact) SignalProducerClass: Class
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

--- @class (exact) SignalWatcherClass: Class
--- @field public new       fun(self: self): SignalWatcher
--- @field private _current SignalWatcher?
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
function SignalWatcher.prototype:__init()
    self._producers = {}
end

--- @public
--- @return void
function SignalWatcher.prototype:activate()
    assert(self._parent == nil, "Watcher is already activated")

    self._parent = SignalWatcher._current
    SignalWatcher._current = self
end

--- @public
--- @return void
function SignalWatcher.prototype:deactivate()
    assert(SignalWatcher._current == self, "Watcher is not activated")

    SignalWatcher._current = self._parent
    self._parent = nil
    self._producers = {}
end

--- @public
--- @return table<SignalProducer, true>
function SignalWatcher.prototype:producers()
    return self._producers
end

--- @public
--- @param producer SignalProducer
--- @return void
function SignalWatcher.prototype:watch(producer)
    self._producers[producer] = true
end

--- @class (exact) ISignal<T>
--- @field get fun(self: self): T

--- @class (exact) IWritableSignal<T>: ISignal<T>
--- @field set fun(self: self, new_value: T): void

--- @class (exact) SignalClass: Class
--- @generic T
--- @field public new fun(self: self, initial_value: T): Signal<T>
local Signal = Class:new("Signal")

--- @class (exact) Signal<T>: IWritableSignal<T>
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

--- @generic T
--- @public
--- @return T
function Signal.prototype:get()
    local watcher = SignalWatcher:current()
    if watcher then
        watcher:watch(self._producer)
    end

    return self._value
end

--- @generic T
--- @public
--- @param new_value T
--- @return void
function Signal.prototype:set(new_value)
    local has_changed = self._value ~= new_value

    self._value = new_value
    if has_changed then
        self._producer:batch_notify()
    end
end

--- @generic T
--- @param initial_value T
--- @return IWritableSignal<T>
function M.signal(initial_value)
    return Signal:new(initial_value)
end

--- @class (exact) ComputedSignalClass: Class
--- @field public new fun(self: self, compute: (fun(): any)): ComputedSignal<any>
local ComputedSignal = Class:new("ComputedSignal")

--- @class (exact) ComputedSignal<T>: ISignal<T>, ISignalConsumer
--- @field private _own_watcher  SignalWatcher
--- @field private _producers    table<SignalProducer, true>
--- @field private _own_producer SignalProducer
--- @field private _compute      fun(): T
--- @field private _has_value    boolean
--- @field private _value?        T
--- @field private _error?        unknown
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

--- @generic T
--- @public
--- @return T
function ComputedSignal.prototype:get()
    local watcher = SignalWatcher:current()
    if watcher then
        watcher:watch(self._own_producer)
    end

    if self._has_value then
        return self._value
    end

    error(self._error, 0)
end

--- @private
--- @return void
function ComputedSignal.prototype:_notify()
    self._own_watcher:activate()
    local ok, value = pcall(self._compute)
    local producers = self._own_watcher:producers()
    self._own_watcher:deactivate()

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
        if not producers[old_producer] then
            old_producer:unsubscribe(self)
        end
    end

    for new_producer in pairs(producers) do
        if not self._producers[new_producer] then
            new_producer:subscribe(self)
        end
    end

    self._producers = producers

    if has_changed then
        self._own_producer:notify()
    end
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

--- @class (exact) EffectClass: Class
--- @field public new fun(self: self, setup: EffectSetup): Effect
local Effect = Class:new("Effect")

--- @class (exact) Effect: IDisposable, ISignalConsumer
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

--- @public
--- @return boolean
function Effect.prototype:is_disposed()
    return self._watcher == nil
end

--- @public
--- @return void
function Effect.prototype:dispose()
    if self:is_disposed() then
        return
    end

    self._watcher = nil
    self._setup = nil
    self:_teardown()

    for producer in pairs(self._producers) do
        producer:unsubscribe(self)
    end

    self._producers = nil
end

--- @private
--- @return void
function Effect.prototype:_notify()
    if self:is_disposed() then
        return
    end

    self:_teardown()

    self._disposables = CompositeDisposable:new()
    local disposer = Disposer:new(self._disposables)

    self._watcher:activate()
    --- @type IEffectScope
    local scope = { disposables = disposer }
    local ok = pcall(self._setup, scope)
    local producers = self._watcher:producers()
    self._watcher:deactivate()

    if not ok then
        self:_teardown()
    end

    for old_producer in pairs(self._producers) do
        if not producers[old_producer] then
            old_producer:unsubscribe(self)
        end
    end

    for new_producer in pairs(producers) do
        if not self._producers[new_producer] then
            new_producer:subscribe(self)
        end
    end

    self._producers = producers
end

--- @private
--- @return void
function Effect.prototype:_teardown()
    local disposables = self._disposables
    self._disposables = nil

    if disposables then
        pcall(disposables.dispose, disposables)
    end
end

--- @param setup EffectSetup
--- @return Effect
function M.effect(setup)
    return Effect:new(setup)
end

return M
