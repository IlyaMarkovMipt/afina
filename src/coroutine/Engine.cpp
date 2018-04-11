#include <afina/coroutine/Engine.h>

#include <setjmp.h>
#include <stdio.h>
#include <string.h>

namespace Afina {
namespace Coroutine {

void Engine::Store(context &ctx) {
    char base;

    ctx.Hight = ctx.Low = StackBottom;
    if (ctx.Low > &base) {
        ctx.Low = &base;
    } else {
        ctx.Hight = &base;
    }

    // allocate if needed
    size_t size = ctx.Hight - ctx.Low;
    char *buf = std::get<0>(ctx.Stack);
    if (std::get<1>(ctx.Stack) < size || buf == nullptr) {
        delete []buf;
        buf = new char[size];
    }

    // storing the stack
    memcpy(buf, ctx.Low, size);
    std::get<0>(ctx.Stack) = buf;
    std::get<1>(ctx.Stack) = size;
}

void Engine::Restore(context &ctx) {
    char base;

    char *basep = &base;
    if ( ctx.Low <= basep && basep <= ctx.Hight) {
        Restore(ctx);
    }

    memcpy(ctx.Low, std::get<0>(ctx.Stack), std::get<1>(ctx.Stack));
    longjmp(ctx.Environment, 1);
}

void Engine::yield() {
    if (alive == nullptr) {
        return;
    }

    context *head = alive;
    while (head) {
        if (head == cur_routine || head->callee || head->State == ROUTINE_DEAD) {
            head = head->next;
        } else {
            // we found not blocked context
            sched(head);
        }
    }
}

void Engine::sched(void *routine) {
    if (routine == nullptr) {
        // try give to caller
        if (cur_routine) {
            if (cur_routine->caller) {
                sched(cur_routine->caller);
            } else {
                yield();
            }
        }
        // otherwise nothing
        return;
    }

    context *ctx = (context *) routine;

    if (ctx->State == ROUTINE_DEAD) {
        fprintf(stderr, "sched on the dead routine\n");
    }
    // save current if it exists
    if (cur_routine) {
        if (setjmp(cur_routine->Environment) == 0) {
            // we should store everything
            Store(*cur_routine);

            if (cur_routine->caller == ctx) {
                ctx->callee = cur_routine->caller = nullptr;
            } else {
                ctx->caller = cur_routine;
                cur_routine->callee = ctx;
            }
        } else {
            return;
        }
    }
    if (cur_routine) {
        cur_routine->State = ROUTINE_SUSPENDED;
    }
    // restore that and jump
    cur_routine = ctx;
    cur_routine->State = ROUTINE_RUNNING;
    Restore(*ctx);
}

} // namespace Coroutine
} // namespace Afina
