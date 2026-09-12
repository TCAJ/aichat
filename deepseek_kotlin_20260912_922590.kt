package com.example.aichat

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

class ChatViewModel : ViewModel() {

    private val api = ChatApi(BuildConfig.DEEPSEEK_API_KEY)

    private val _messages = MutableStateFlow<List<ChatMessage>>(emptyList())
    val messages: StateFlow<List<ChatMessage>> = _messages.asStateFlow()

    private var job: Job? = null

    fun send(text: String) {
        val input = text.trim()
        if (input.isEmpty() || job?.isActive == true) return

        _messages.update {
            it + ChatMessage("user", input) + ChatMessage("assistant", "")
        }

        job = viewModelScope.launch {
            try {
                val history = _messages.value.dropLast(1)
                api.stream(history).collect { chunk ->
                    _messages.update { list ->
                        val last = list.last()
                        list.dropLast(1) + last.copy(content = last.content + chunk)
                    }
                }
            } catch (e: Exception) {
                _messages.update { list ->
                    val last = list.last()
                    val msg = last.content.ifEmpty {
                        "请求失败：${e.message}"
                    }
                    list.dropLast(1) + last.copy(content = msg)
                }
            }
        }
    }

    fun clear() {
        job?.cancel()
        _messages.value = emptyList()
    }
}