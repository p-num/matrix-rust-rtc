// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.matrix.rtc.*
import com.sun.jna.Callback
import com.sun.jna.CallbackReference
import com.sun.jna.CallbackThreadInitializer

private const val ROOM = "!factory:example.org"
private const val SLOT = "m.call#room"
private const val STATE_MEMBER = "org.matrix.msc3401.call.member"

private data class Send(val sequence: Int, val endpoint: String, val type: String, val content: String) {
    val joining get() = content.contains("\"membership\":\"join\"") || content.contains("\"io.letro.call.join_id\"")
    val membership get() = !type.startsWith("delayed:") && (type.endsWith("rtc.member") || type == STATE_MEMBER)
}

private class Sends {
    private val sequence = AtomicInteger()
    val values = CopyOnWriteArrayList<Send>()
    private val events = Channel<Send>(Channel.UNLIMITED)
    fun record(endpoint: String, type: String, content: String): String {
        val send = Send(sequence.incrementAndGet(), endpoint, type, content)
        values += send
        check(events.trySend(send).isSuccess)
        return "\$event-${send.sequence}"
    }
    fun joins(endpoint: String) = values.filter { it.endpoint == endpoint && it.membership && it.joining }
    suspend fun await(predicate: (Send) -> Boolean) = withTimeout(6_000) {
        while (true) {
            if (predicate(events.receive())) return@withTimeout
        }
    }
}

private class Host(private val endpoint: String, private val sends: Sends, private val denied: Boolean = false) : MatrixBackend {
    override fun ownUserId() = "@alice:example.org"
    override fun ownDeviceId() = "DEVICE"
    private fun send(type: String, content: String): String {
        if (denied) throw FfiBackendException.Failed("M_FORBIDDEN", 403u.toUShort(), null, true, "scope denied")
        return sends.record(endpoint, type, content)
    }
    override suspend fun sendStickyEvent(roomId: String, eventType: String, contentJson: String, durationMs: ULong) = send(eventType, contentJson)
    override suspend fun sendStateEvent(roomId: String, eventType: String, stateKey: String, contentJson: String) = send(eventType, contentJson)
    override suspend fun sendRoomEvent(roomId: String, eventType: String, contentJson: String) = send(eventType, contentJson)
    override suspend fun sendDelayedStateEvent(roomId: String, eventType: String, stateKey: String, contentJson: String, delayMs: ULong) = send("delayed:$eventType", contentJson)
    override suspend fun sendDelayedStickyEvent(roomId: String, eventType: String, contentJson: String, stickyDurationMs: ULong, delayMs: ULong) = send("delayed:$eventType", contentJson)
    override suspend fun restartDelayedEvent(roomId: String, delayId: String) = Unit
    override suspend fun cancelDelayedEvent(roomId: String, delayId: String) = Unit
    override suspend fun redactEvent(roomId: String, eventId: String, reason: String?) = Unit
    override suspend fun sendToDeviceMessage(recipients: List<FfiToDeviceRecipient>, messageType: String, contentJson: String) =
        recipients.map { FfiToDeviceDelivery(it.userId, it.deviceId, null) }
    override suspend fun relations(roomId: String, eventId: String, relType: String, eventType: String) = emptyList<FfiEventIn>()
    override suspend fun openidToken() = FfiOpenIdToken("fixture-token", "Bearer", "example.org", 300uL)
    override suspend fun livekitToken(request: FfiLiveKitTokenRequest) = "fixture-jwt"
    override suspend fun rtcTransports() = "[]"
    override fun subscribeToDevice(eventTypes: List<String>, sink: ToDeviceSink) = object : BackendSubscription { override fun cancel() = Unit }
    override fun subscribeRoom(roomId: String, subjects: FfiRoomSubjects, sink: RoomSink): BackendSubscription {
        sink.onEncryption(false)
        if ("m.rtc.slot" in subjects.stateEventTypes) {
            sink.onStateEvents("m.rtc.slot", listOf(FfiEventIn(
                eventId = "\$slot", sender = ownUserId(), eventType = "m.rtc.slot", stateKey = SLOT,
                contentJson = """{"status":"open","application":{"type":"m.call"}}""",
                encryption = FfiEventEncryption(false),
            )))
        }
        sink.onJoinedMembers(listOf(ownUserId(), "@bob:example.org"))
        sink.onStickyEvents(emptyList())
        if (STATE_MEMBER in subjects.stateEventTypes) sink.onStateEvents(STATE_MEMBER, emptyList())
        return object : BackendSubscription { override fun cancel() = Unit }
    }
}

private class Factory(private val sends: Sends) : MembershipBackendFactory {
    val endpoints = ConcurrentHashMap<String, MatrixBackend>()
    val lookups = CopyOnWriteArrayList<Pair<String, String?>>()
    private val denied = Host("denied", sends, denied = true)
    fun register(id: String, label: String) { endpoints[id] = Host(label, sends) }
    override fun forMembership(roomId: String, requestScopeId: String?): MatrixBackend {
        lookups += roomId to requestScopeId
        if (requestScopeId == "throw") error("factory failed deliberately")
        return requestScopeId?.let { endpoints[it] } ?: denied
    }
}

private fun params(id: String? = null) = FfiJoinSessionParams(
    requestScopeId = id,
    transport = FfiJoinTransport.Publish("wss://sfu.example.org", "https://sfu.example.org"),
    kind = FfiCallKind.Group(null), keepAliveTimeoutMs = null, stickyDurationMs = null, encryptionConfig = null,
)

private suspend fun leave(call: RtcCall, room: RtcRoom) {
    call.leave(FfiLeaveSessionParams(null))
    call.close()
    room.shutdown()
    room.close()
}

private suspend fun capturedViews(format: FfiMembershipFormat) {
    val sends = Sends()
    val factory = Factory(sends)
    factory.register("local-R1", "R1-view")
    val client = RtcClient.newWithMembershipFactory(Host("root", sends), factory)
    val firstRoom = client.room(ROOM, FfiRoomOptions(format))
    val first = firstRoom.joinCall(params("local-R1"))
    check(sends.joins("R1-view").size == 1)
    factory.endpoints.remove("local-R1")
    factory.register("local-R2", "R2-view")
    System.gc()
    leave(first, firstRoom)
    sends.await { it.endpoint == "R1-view" && it.membership && !it.joining }
    check(factory.lookups.count { it.second == "local-R1" } == 1)
    val secondRoom = client.room(ROOM, FfiRoomOptions(format))
    val second = secondRoom.joinCall(params("local-R2"))
    check(sends.joins("R2-view").size == 1)
    check(sends.joins("root").isEmpty())
    check(factory.lookups.size == 2)
    check(sends.values.none { "local-R1" in it.content || "local-R2" in it.content })
    leave(second, secondRoom)
    client.close()
    println("FOREIGN_FACTORY_CAPTURE $format PASS lookups=2 leave=R1-view replacement=R2-view rootJoins=0")
}

private suspend fun deniedScopes(format: FfiMembershipFormat) {
    for (scope in listOf(null, "unknown", "retired")) {
        val sends = Sends()
        val factory = Factory(sends)
        factory.register("retired", "retired-view")
        factory.endpoints.remove("retired")
        val client = RtcClient.newWithMembershipFactory(Host("root", sends), factory)
        val room = client.room(ROOM, FfiRoomOptions(format))
        var failure: Throwable? = null
        try { room.joinCall(params(scope)) } catch (error: Throwable) { failure = error }
        check(failure is MatrixRtcFfiException.JoinRefused) { "expected denied join, got $failure" }
        check(factory.lookups.single().second == scope)
        check(sends.values.isEmpty())
        room.shutdown(); room.close(); client.close()
    }
    println("FOREIGN_FACTORY_DENIAL $format PASS absent/unknown/retired rootWrites=0")
}

private suspend fun unexpectedFactoryFailure(format: FfiMembershipFormat) {
    val sends = Sends()
    val factory = Factory(sends)
    factory.register("active-R2", "current-R2-view")
    val client = RtcClient.newWithMembershipFactory(Host("root", sends), factory)
    val room = client.room(ROOM, FfiRoomOptions(format))
    var failure: Throwable? = null
    try { room.joinCall(params("throw")) } catch (error: Throwable) { failure = error }
    check(failure != null) { "factory exception must not authorize a join" }
    check(sends.values.isEmpty()) { "factory exception used root or current backend" }
    println("FOREIGN_FACTORY_THROW $format ${checkNotNull(failure)::class.java.name}: ${checkNotNull(failure).message}")
    val valid = room.joinCall(params("active-R2"))
    check(sends.joins("current-R2-view").size == 1)
    leave(valid, room)
    client.close()
    println("FOREIGN_FACTORY_RECOVERY $format PASS rootWrites=0 processAlive=true")
}

private suspend fun legacyConstructor(format: FfiMembershipFormat) {
    val sends = Sends()
    val client = RtcClient(Host("legacy-root", sends))
    val room = client.room(ROOM, FfiRoomOptions(format))
    val call = room.joinCall(params("ignored-local-context"))
    check(sends.joins("legacy-root").size == 1)
    check(sends.values.none { "ignored-local-context" in it.content })
    leave(call, room); client.close()
    println("FOREIGN_FACTORY_LEGACY_CONSTRUCTOR $format PASS")
}

private fun factoryCallbacksStayAttached() {
    MatrixRtc.initialize()
    MatrixRtc.initialize()
    // Inspect the real JNA policy installed by the Android binding initializer.
    val field = CallbackReference::class.java.getDeclaredField("initializers")
    field.isAccessible = true
    val initializers = field.get(null) as Map<*, *>
    val table = uniffiCallbackInterfaceMembershipBackendFactory.vtable
    val callbacks = table.javaClass.fields.mapNotNull { it.get(table) as? Callback }
    check(callbacks.isNotEmpty())
    for (callback in callbacks) {
        val initializer = initializers[callback] as? CallbackThreadInitializer
            ?: error("factory callback has no thread initializer")
        check(!initializer.detach(callback)) { "factory callback would detach a shared native thread" }
        check(initializer.isDaemon(callback))
    }
    println("FOREIGN_FACTORY_CALLBACK_THREADS PASS callbacks=${callbacks.size} detach=false")
}

fun main() = runBlocking {
    factoryCallbacksStayAttached()
    for (format in listOf(FfiMembershipFormat.MSC, FfiMembershipFormat.ROOM_STATE)) {
        legacyConstructor(format)
        capturedViews(format)
        deniedScopes(format)
        unexpectedFactoryFailure(format)
    }
    println("FOREIGN_FACTORY_ROUNDTRIP PASS modern+legacy actualReturnedKotlinBackends=true")
}
