package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Lega;
import it.costantino.astaapp.repository.LegaRepository;
import it.costantino.astaapp.service.LegaService;
import it.costantino.astaapp.service.dto.LegaDTO;
import it.costantino.astaapp.service.mapper.LegaMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Lega}.
 */
@Service
@Transactional
public class LegaServiceImpl implements LegaService {

    private static final Logger LOG = LoggerFactory.getLogger(LegaServiceImpl.class);

    private final LegaRepository legaRepository;

    private final LegaMapper legaMapper;

    public LegaServiceImpl(LegaRepository legaRepository, LegaMapper legaMapper) {
        this.legaRepository = legaRepository;
        this.legaMapper = legaMapper;
    }

    @Override
    public LegaDTO save(LegaDTO legaDTO) {
        LOG.debug("Request to save Lega : {}", legaDTO);
        Lega lega = legaMapper.toEntity(legaDTO);
        lega = legaRepository.save(lega);
        return legaMapper.toDto(lega);
    }

    @Override
    public LegaDTO update(LegaDTO legaDTO) {
        LOG.debug("Request to update Lega : {}", legaDTO);
        Lega lega = legaMapper.toEntity(legaDTO);
        lega = legaRepository.save(lega);
        return legaMapper.toDto(lega);
    }

    @Override
    public Optional<LegaDTO> partialUpdate(LegaDTO legaDTO) {
        LOG.debug("Request to partially update Lega : {}", legaDTO);

        return legaRepository
            .findById(legaDTO.getId())
            .map(existingLega -> {
                legaMapper.partialUpdate(existingLega, legaDTO);

                return existingLega;
            })
            .map(legaRepository::save)
            .map(legaMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<LegaDTO> findAll() {
        LOG.debug("Request to get all Legas");
        return legaRepository.findAll().stream().map(legaMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<LegaDTO> findOne(Long id) {
        LOG.debug("Request to get Lega : {}", id);
        return legaRepository.findById(id).map(legaMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Lega : {}", id);
        legaRepository.deleteById(id);
    }
}
