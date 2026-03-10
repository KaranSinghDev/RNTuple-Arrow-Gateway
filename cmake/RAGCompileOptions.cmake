function(rag_set_compile_options target)
  target_compile_features(${target} PRIVATE cxx_std_17)
  target_compile_options(${target} PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-parameter
  )
  if(RAG_WERROR)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
  if(RAG_SANITIZE)
    target_compile_options(${target} PRIVATE
      -fsanitize=address,undefined
      -fno-omit-frame-pointer
    )
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  endif()
endfunction()
