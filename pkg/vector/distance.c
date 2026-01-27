#include <math.h>
#include <stdint.h>
#include <string.h>

// Function pointers (initialized at runtime)
float (*distance_l2_func)(const float *, const float *, int) = NULL;
float (*distance_cosine_func)(const float *, const float *, int) = NULL;
float (*distance_dot_func)(const float *, const float *, int) = NULL;

// Track which backend is being used
static const char *current_backend = "uninitialized";

// ============================================================================
// Scalar Implementations (Always Available)
// ============================================================================

float distance_l2_scalar(const float *a, const float *b, int dims)
{
	float sum = 0.0f;

	for (int i = 0; i < dims; i++)
	{
		float diff = a[i] - b[i];
		sum += diff * diff;
	}

	return sqrtf(sum);
}

float distance_cosine_scalar(const float *a, const float *b, int dims)
{
	float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;

	for (int i = 0; i < dims; i++)
	{
		dot += a[i] * b[i];
		norm_a += a[i] * a[i];
		norm_b += b[i] * b[i];
	}

	if (norm_a == 0.0f || norm_b == 0.0f)
	{
		return 1.0f; // Maximum distance for zero vectors
	}

	float similarity = dot / (sqrtf(norm_a) * sqrtf(norm_b));
	return 1.0f - similarity;
}

float distance_dot_scalar(const float *a, const float *b, int dims)
{
	float dot = 0.0f;

	for (int i = 0; i < dims; i++)
	{
		dot += a[i] * b[i];
	}

	return -dot; // Negative for use with max-heap
}

// ============================================================================
// AVX2 Implementations
// ============================================================================

#if defined(__AVX2__)
#include <immintrin.h>

float distance_l2_avx2(const float *a, const float *b, int dims)
{
	__m256 sum_vec = _mm256_setzero_ps();
	int i = 0;

	// Process 8 floats at a time
	for (; i + 7 < dims; i += 8)
	{
		__m256 a_vec = _mm256_loadu_ps(&a[i]);
		__m256 b_vec = _mm256_loadu_ps(&b[i]);
		__m256 diff = _mm256_sub_ps(a_vec, b_vec);
		sum_vec = _mm256_fmadd_ps(diff, diff, sum_vec);
	}

	// Horizontal sum
	float result[8];
	_mm256_storeu_ps(result, sum_vec);
	float sum = result[0] + result[1] + result[2] + result[3] +
				result[4] + result[5] + result[6] + result[7];

	// Handle remaining elements
	for (; i < dims; i++)
	{
		float diff = a[i] - b[i];
		sum += diff * diff;
	}

	return sqrtf(sum);
}

float distance_cosine_avx2(const float *a, const float *b, int dims)
{
	__m256 dot_vec = _mm256_setzero_ps();
	__m256 norm_a_vec = _mm256_setzero_ps();
	__m256 norm_b_vec = _mm256_setzero_ps();
	int i = 0;

	// Process 8 floats at a time
	for (; i + 7 < dims; i += 8)
	{
		__m256 a_vec = _mm256_loadu_ps(&a[i]);
		__m256 b_vec = _mm256_loadu_ps(&b[i]);

		dot_vec = _mm256_fmadd_ps(a_vec, b_vec, dot_vec);
		norm_a_vec = _mm256_fmadd_ps(a_vec, a_vec, norm_a_vec);
		norm_b_vec = _mm256_fmadd_ps(b_vec, b_vec, norm_b_vec);
	}

	// Horizontal sum
	float dot_result[8], norm_a_result[8], norm_b_result[8];
	_mm256_storeu_ps(dot_result, dot_vec);
	_mm256_storeu_ps(norm_a_result, norm_a_vec);
	_mm256_storeu_ps(norm_b_result, norm_b_vec);

	float dot = dot_result[0] + dot_result[1] + dot_result[2] + dot_result[3] +
				dot_result[4] + dot_result[5] + dot_result[6] + dot_result[7];
	float norm_a = norm_a_result[0] + norm_a_result[1] + norm_a_result[2] + norm_a_result[3] +
				   norm_a_result[4] + norm_a_result[5] + norm_a_result[6] + norm_a_result[7];
	float norm_b = norm_b_result[0] + norm_b_result[1] + norm_b_result[2] + norm_b_result[3] +
				   norm_b_result[4] + norm_b_result[5] + norm_b_result[6] + norm_b_result[7];

	// Handle remaining elements
	for (; i < dims; i++)
	{
		dot += a[i] * b[i];
		norm_a += a[i] * a[i];
		norm_b += b[i] * b[i];
	}

	if (norm_a == 0.0f || norm_b == 0.0f)
	{
		return 1.0f;
	}

	float similarity = dot / (sqrtf(norm_a) * sqrtf(norm_b));
	return 1.0f - similarity;
}

float distance_dot_avx2(const float *a, const float *b, int dims)
{
	__m256 dot_vec = _mm256_setzero_ps();
	int i = 0;

	// Process 8 floats at a time
	for (; i + 7 < dims; i += 8)
	{
		__m256 a_vec = _mm256_loadu_ps(&a[i]);
		__m256 b_vec = _mm256_loadu_ps(&b[i]);
		dot_vec = _mm256_fmadd_ps(a_vec, b_vec, dot_vec);
	}

	// Horizontal sum
	float result[8];
	_mm256_storeu_ps(result, dot_vec);
	float dot = result[0] + result[1] + result[2] + result[3] +
				result[4] + result[5] + result[6] + result[7];

	// Handle remaining elements
	for (; i < dims; i++)
	{
		dot += a[i] * b[i];
	}

	return -dot;
}
#endif

// ============================================================================
// SSE2 Implementations
// ============================================================================

#if defined(__SSE2__)
#include <emmintrin.h>

float distance_l2_sse2(const float *a, const float *b, int dims)
{
	__m128 sum_vec = _mm_setzero_ps();
	int i = 0;

	// Process 4 floats at a time
	for (; i + 3 < dims; i += 4)
	{
		__m128 a_vec = _mm_loadu_ps(&a[i]);
		__m128 b_vec = _mm_loadu_ps(&b[i]);
		__m128 diff = _mm_sub_ps(a_vec, b_vec);
		__m128 squared = _mm_mul_ps(diff, diff);
		sum_vec = _mm_add_ps(sum_vec, squared);
	}

	// Horizontal sum
	float result[4];
	_mm_storeu_ps(result, sum_vec);
	float sum = result[0] + result[1] + result[2] + result[3];

	// Handle remaining elements
	for (; i < dims; i++)
	{
		float diff = a[i] - b[i];
		sum += diff * diff;
	}

	return sqrtf(sum);
}

float distance_cosine_sse2(const float *a, const float *b, int dims)
{
	__m128 dot_vec = _mm_setzero_ps();
	__m128 norm_a_vec = _mm_setzero_ps();
	__m128 norm_b_vec = _mm_setzero_ps();
	int i = 0;

	// Process 4 floats at a time
	for (; i + 3 < dims; i += 4)
	{
		__m128 a_vec = _mm_loadu_ps(&a[i]);
		__m128 b_vec = _mm_loadu_ps(&b[i]);

		dot_vec = _mm_add_ps(dot_vec, _mm_mul_ps(a_vec, b_vec));
		norm_a_vec = _mm_add_ps(norm_a_vec, _mm_mul_ps(a_vec, a_vec));
		norm_b_vec = _mm_add_ps(norm_b_vec, _mm_mul_ps(b_vec, b_vec));
	}

	// Horizontal sum
	float dot_result[4], norm_a_result[4], norm_b_result[4];
	_mm_storeu_ps(dot_result, dot_vec);
	_mm_storeu_ps(norm_a_result, norm_a_vec);
	_mm_storeu_ps(norm_b_result, norm_b_vec);

	float dot = dot_result[0] + dot_result[1] + dot_result[2] + dot_result[3];
	float norm_a = norm_a_result[0] + norm_a_result[1] + norm_a_result[2] + norm_a_result[3];
	float norm_b = norm_b_result[0] + norm_b_result[1] + norm_b_result[2] + norm_b_result[3];

	// Handle remaining elements
	for (; i < dims; i++)
	{
		dot += a[i] * b[i];
		norm_a += a[i] * a[i];
		norm_b += b[i] * b[i];
	}

	if (norm_a == 0.0f || norm_b == 0.0f)
	{
		return 1.0f;
	}

	float similarity = dot / (sqrtf(norm_a) * sqrtf(norm_b));
	return 1.0f - similarity;
}

float distance_dot_sse2(const float *a, const float *b, int dims)
{
	__m128 dot_vec = _mm_setzero_ps();
	int i = 0;

	// Process 4 floats at a time
	for (; i + 3 < dims; i += 4)
	{
		__m128 a_vec = _mm_loadu_ps(&a[i]);
		__m128 b_vec = _mm_loadu_ps(&b[i]);
		dot_vec = _mm_add_ps(dot_vec, _mm_mul_ps(a_vec, b_vec));
	}

	// Horizontal sum
	float result[4];
	_mm_storeu_ps(result, dot_vec);
	float dot = result[0] + result[1] + result[2] + result[3];

	// Handle remaining elements
	for (; i < dims; i++)
	{
		dot += a[i] * b[i];
	}

	return -dot;
}
#endif

// ============================================================================
// ARM NEON Implementations
// ============================================================================

#if defined(__ARM_NEON)
#include <arm_neon.h>

float distance_l2_neon(const float *a, const float *b, int dims)
{
	float32x4_t sum_vec = vdupq_n_f32(0.0f);
	int i = 0;

	// Process 4 floats at a time
	for (; i + 3 < dims; i += 4)
	{
		float32x4_t a_vec = vld1q_f32(&a[i]);
		float32x4_t b_vec = vld1q_f32(&b[i]);
		float32x4_t diff = vsubq_f32(a_vec, b_vec);
		sum_vec = vfmaq_f32(sum_vec, diff, diff);
	}

	// Horizontal sum
	float sum = vaddvq_f32(sum_vec);

	// Handle remaining elements
	for (; i < dims; i++)
	{
		float diff = a[i] - b[i];
		sum += diff * diff;
	}

	return sqrtf(sum);
}

float distance_cosine_neon(const float *a, const float *b, int dims)
{
	float32x4_t dot_vec = vdupq_n_f32(0.0f);
	float32x4_t norm_a_vec = vdupq_n_f32(0.0f);
	float32x4_t norm_b_vec = vdupq_n_f32(0.0f);
	int i = 0;

	// Process 4 floats at a time
	for (; i + 3 < dims; i += 4)
	{
		float32x4_t a_vec = vld1q_f32(&a[i]);
		float32x4_t b_vec = vld1q_f32(&b[i]);

		dot_vec = vfmaq_f32(dot_vec, a_vec, b_vec);
		norm_a_vec = vfmaq_f32(norm_a_vec, a_vec, a_vec);
		norm_b_vec = vfmaq_f32(norm_b_vec, b_vec, b_vec);
	}

	// Horizontal sum
	float dot = vaddvq_f32(dot_vec);
	float norm_a = vaddvq_f32(norm_a_vec);
	float norm_b = vaddvq_f32(norm_b_vec);

	// Handle remaining elements
	for (; i < dims; i++)
	{
		dot += a[i] * b[i];
		norm_a += a[i] * a[i];
		norm_b += b[i] * b[i];
	}

	if (norm_a == 0.0f || norm_b == 0.0f)
	{
		return 1.0f;
	}

	float similarity = dot / (sqrtf(norm_a) * sqrtf(norm_b));
	return 1.0f - similarity;
}

float distance_dot_neon(const float *a, const float *b, int dims)
{
	float32x4_t dot_vec = vdupq_n_f32(0.0f);
	int i = 0;

	// Process 4 floats at a time
	for (; i + 3 < dims; i += 4)
	{
		float32x4_t a_vec = vld1q_f32(&a[i]);
		float32x4_t b_vec = vld1q_f32(&b[i]);
		dot_vec = vfmaq_f32(dot_vec, a_vec, b_vec);
	}

	// Horizontal sum
	float dot = vaddvq_f32(dot_vec);

	// Handle remaining elements
	for (; i < dims; i++)
	{
		dot += a[i] * b[i];
	}

	return -dot;
}
#endif

// ============================================================================
// Runtime Detection and Initialization
// ============================================================================

void init_distance_functions()
{
	const char *backend = "scalar";

#if defined(__AVX2__)
// Check for AVX2 support at runtime
#if defined(__GNUC__) || defined(__clang__)
	if (__builtin_cpu_supports("avx2"))
	{
		distance_l2_func = distance_l2_avx2;
		distance_cosine_func = distance_cosine_avx2;
		distance_dot_func = distance_dot_avx2;
		backend = "AVX2";
		goto done;
	}
#endif
#endif

#if defined(__SSE2__)
#if defined(__GNUC__) || defined(__clang__)
	if (__builtin_cpu_supports("sse2"))
	{
		distance_l2_func = distance_l2_sse2;
		distance_cosine_func = distance_cosine_sse2;
		distance_dot_func = distance_dot_sse2;
		backend = "SSE2";
		goto done;
	}
#endif
#endif

#if defined(__ARM_NEON)
	// NEON is always available on ARM64
	distance_l2_func = distance_l2_neon;
	distance_cosine_func = distance_cosine_neon;
	distance_dot_func = distance_dot_neon;
	backend = "NEON";
	goto done;
#endif

done:
	// Fallback to scalar
	if (distance_l2_func == NULL)
	{
		distance_l2_func = distance_l2_scalar;
		distance_cosine_func = distance_cosine_scalar;
		distance_dot_func = distance_dot_scalar;
	}

	current_backend = backend;
}

// ============================================================================
// Public API Functions Called from Go
// ============================================================================

float compute_distance_l2(const float *a, const float *b, int dims)
{
	if (distance_l2_func == NULL)
	{
		init_distance_functions();
	}

	return distance_l2_func(a, b, dims);
}

float compute_distance_cosine(const float *a, const float *b, int dims)
{
	if (distance_cosine_func == NULL)
	{
		init_distance_functions();
	}

	return distance_cosine_func(a, b, dims);
}

float compute_distance_dot(const float *a, const float *b, int dims)
{
	if (distance_dot_func == NULL)
	{
		init_distance_functions();
	}

	return distance_dot_func(a, b, dims);
}
