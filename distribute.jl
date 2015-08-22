# Function creates a distributed array differently from the distribute
# function by creating blocks from the array and distributing each block
# over the workers in chunks. The function returns an array of references
# to each chunk.

@everywhere function Base.distribute(a::AbstractArray, args...)
    owner = myid()
    rr = RemoteRef()
    put!(rr, a)
    DArray(size(a), args...) do I
        remotecall_fetch(owner, ()->fetch(rr)[I...])
    end
end

# TODO: Idiot checks on nrow < blocksize etc
# This function takes an array and a selected chunk size and distributes
# it amongst the workers in blocks. So the block size = nworkers()*_chunk_size
# the chunks in each block are distributed amongst the workers and the
# array is divided into blocks. The function returns an array of remote
# references to the chunks so that algorithms can be run over them
#
@everywhere function distribute_array(_a::AbstractArray, _chunk_size::Int64 = 50000)
	_nrow = size(_a)[1] # number of rows in the array
	_block_size = _chunk_size*nworkers() # number of rows in a block
	_nblocks = div(_nrow, _block_size)
	_rem = rem(_nrow, _block_size)
	_indices::Array{UnitRange{Int64}}
	_y = Array(UnitRange{Int64}, 1)
	_indices = [((_i - 1)*_block_size + 1):_block_size*_i for _i in 1:_nblocks]
	if _rem > 0
		_y[1] = (_nblocks*_block_size):_nrow
		_indices = [_indices, _y]
	end
	_refs = Array(RemoteRef, 0)
	[_refs = [_refs, distribute(_a[_i, :], workers(), [nworkers(), 1]).chunks] for _i in _indices]
	return _refs
end


# Function to append data to a remote reference distributed array
@everywhere append!(_r::Array{RemoteRef}, _a::AbstractArray, _args...)
	_refs = distribute_array(_a, _args...)
	_r = [_r, _refs]
	return _r
end


# Turns a regular function into a function that accepts a remote reference
# does a fetch and executes that function (runs locally on wherever you
# are executing the function)
@everywhere function create_mapper_fun(fun::Function, args...)
	function _fun(_x::RemoteRef, args...)
		_y = fetch(_x)
		return fun(_y, args...)
	end
	return _fun
end

# Next a map agorithm that runs a function over a remote reference array
# and returns another remote reference array to the output.
@everywhere function parallel_map(_fun::Function, _a::Array{RemoteRef}, args...)
	_out = Array(RemoteRef, 0)
	for _i in _a
		_proc = _i.where
		_out = [_out, [remotecall(_proc, _fun, _i, args...)]]
	end
	_out
end

# Higher order function to create the reducer function that accepts 
# remote references. I can see that this will leave a trail of artifacts
# on the RemoteRef that get created on _out = RemoteRef() - memory hungry.
@everywhere function create_reducer_function(fun::Function)
	function _fun(_x::RemoteRef, _y::RemoteRef)
		_out = RemoteRef()
		x = fetch(_x)
		y = fetch(_y)
		_z = fun(x, y)
		put!(_out, _z)
		return _out
	end
	return _fun
end


# Function for a serial reducer
@everywhere function serial_reduce(_fun::Function, _a::Array{RemoteRef})
	_out = fetch(_a[1])
	for _i in _a[2:end]
		_out = _fun(_out, fetch(_i))
	end
	return _out
end

# Parallel Reducer
@everywhere function parallel_reduce(_fun::Function, _a::Array{RemoteRef})
	_out = @parallel (_fun) for _i in _a
		_i
	end
	return take!(_out) # rather than a fetch() to leave it empty
end
